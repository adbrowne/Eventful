namespace Eventful.Raven

open System
open System.Threading
open System.Runtime.Caching

open Eventful
open Metrics

open FSharpx

open Raven.Client
open Raven.Abstractions.Data
open Raven.Json.Linq

type BulkRavenProjector<'TMessage when 'TMessage :> IBulkRavenMessage> 
    (
        documentStore:Raven.Client.IDocumentStore, 
        processors:ProcessorSet<'TMessage>,
        databaseName: string,
        maxEventQueueSize : int,
        eventWorkers: int,
        maxWriterQueueSize: int,
        writerWorkers: int,
        onEventComplete : 'TMessage -> Async<unit>,
        cancellationToken : CancellationToken
    ) =

    let log = Common.Logging.LogManager.GetLogger(typeof<BulkRavenProjector<_>>)

    let cache = new MemoryCache("RavenBatchWrite-" + databaseName)

    let batchWriteTracker = Metric.Histogram(sprintf "BatchWriteSize %s" databaseName, Unit.Items)
    let completeItemsTracker = Metric.Meter(sprintf "EventsComplete %s" databaseName, Unit.Items)
    let processingExceptions = Metric.Meter(sprintf "ProcessingExceptions %s" databaseName, Unit.Items)
    let batchWriteTime = Metric.Timer(sprintf "WriteTime %s" databaseName, Unit.None)
    let batchWritesMeter = Metric.Meter(sprintf "BatchWrites %s" databaseName, Unit.Items)
    let batchConflictsMeter = Metric.Meter(sprintf "BatchConflicts %s" databaseName, Unit.Items)

    let writeDocs docs = async {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let batchId = Guid.NewGuid()
        let originalDocMap = 
            docs
            |> Seq.collect (fun (writeRequests, callback) -> 
                writeRequests
                |> Seq.map(fun processAction ->
                    match processAction with
                    | Write ({ DocumentKey = key; Document = document; Metadata = metadata }, _) ->
                        let document = document
                        (key, Choice1Of2 (document, metadata.Force(), callback))
                    | Delete ({ DocumentKey = key }, _) ->
                        (key, Choice2Of2 ())
                )
            )
            |> Map.ofSeq

        let! result = BatchOperations.writeBatch documentStore databaseName docs
        let writeSuccessful = 
            match result with
            | Some (batchResult, docs) ->
                batchWriteTracker.Update(batchResult.LongLength)
                batchWritesMeter.Mark()
                for docResult in batchResult do
                    match originalDocMap.[docResult.Key] with
                    | Choice1Of2 (doc, metadata, callback) ->
                        let cacheKey = RavenOperations.getCacheKey databaseName docResult.Key
                        cache.Set(cacheKey, (doc, metadata, docResult.Etag) :> obj, DateTimeOffset.MaxValue) |> ignore
                    | Choice2Of2 _ ->
                        let cacheKey = RavenOperations.getCacheKey databaseName docResult.Key
                        cache.Remove(cacheKey) |> ignore

                true
            | None ->
                batchConflictsMeter.Mark()
                for docRequest in originalDocMap |> Map.toSeq do
                    match docRequest with
                    | (docKey, Choice1Of2 (_, _, callback)) ->
                        let cacheKey = RavenOperations.getCacheKey databaseName docKey
                        cache.Remove(cacheKey) |> ignore
                    | _ -> ()
                false
        
        for (docs, callback) in docs do

            do! callback writeSuccessful

        sw.Stop()
        batchWriteTime.Record(sw.ElapsedMilliseconds, TimeUnit.Milliseconds)
    }

    let writeBatch _ (docs : seq<BatchWrite>) = async {
        let maxWriteSize = 100
        let writeGroups = 
            docs
            |> Seq.zip (Seq.initInfinite id)
            |> Seq.groupBy (fun (i,d) -> i / maxWriteSize)

        for (_, w) in writeGroups do
            do! writeDocs (w |> Seq.map snd)
    }

    let writeQueue = 
        let x = new WorktrackingQueue<int, BatchWrite, BatchWrite>((fun a -> (a, Set.singleton 1)), writeBatch, maxWriterQueueSize, writerWorkers, name = databaseName + " write", cancellationToken = cancellationToken) 
        x.StopWork()
        x

    let getPromise () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let complete  = fun success -> async { tcs.SetResult(success) }
        (complete, Async.AwaitTask tcs.Task)
        
    let fetcher = {
        new IDocumentFetcher with
            member x.GetDocument<'TDocument> key =
                runWithTimeout "Single Fetch" 30 <| RavenOperations.getDocument<'TDocument> documentStore cache databaseName key
            member x.GetDocuments request = 
                runWithTimeout "Multi Fetch" 30 <| RavenOperations.getDocuments documentStore cache databaseName request
    }

    let tryEvent (key : IComparable, documentProcessor : UntypedDocumentProcessor<'TMessage>) events =
        async { 
            let untypedKey = key :> obj

            let! writeRequests = documentProcessor.Process fetcher untypedKey events

            let (complete, wait) = getPromise()
                
            do! writeQueue.Add(writeRequests, complete)

            let! result = wait
            return result 
        }
        
    let processEvent key values = async {
        let cachedValues = values |> Seq.cache
        let maxAttempts = 10
        let rec loop count = async {
            if count < maxAttempts then
                try
                    let! attempt = tryEvent key cachedValues
                    if not attempt then
                        return! loop (count + 1)
                    else
                        ()
                with | e ->
                    consoleLog <| sprintf "Exception while processing: %A %A %A %A" e e.StackTrace key values
                    return! loop(count + 1)
            else
                processingExceptions.Mark()
                consoleLog <| sprintf "Processing failed permanently: %A %A" key values
                ()
        }
        do! loop 0
    }

    let grouper (event : 'TMessage) =
        let groups = 
            processors.Items
            |> Seq.collect (fun x -> 
                if x.EventTypes.Contains(event.EventType) then
                    x.MatchingKeys event
                    |> Seq.map (fun k9 -> (k9, x))
                else 
                    Seq.empty)
            |> Set.ofSeq
        (event, groups)
    
    let tracker = new LastCompleteItemAgent2<EventPosition>(name = databaseName)

    let eventComplete (event : 'TMessage) =
        seq {
            let position = event.GlobalPosition
            match position with
            | Some position ->
                yield async {
                    tracker.Complete(position)
                    completeItemsTracker.Mark(1L)
                }
            | None -> ()

            yield onEventComplete event
        }
        |> Async.Parallel
        |> Async.Ignore

    let queue = 
        let q = new WorktrackingQueue<_,_,_>(grouper, processEvent, maxEventQueueSize, eventWorkers, eventComplete, name = databaseName + " processing");
        q.StopWork()
        q

    let positionDocumentKey = "EventProcessingPosition"

    let mutable lastPositionWritten : Option<EventPosition> = None

    member x.LastComplete () = async {
        let getPersistedPosition = async {
            let! (persistedLastComplete : ProjectedDocument<EventPosition> option) = fetcher.GetDocument positionDocumentKey
            return persistedLastComplete |> Option.map((fun (doc,_,_) -> doc))
        }

        let! thisSessionLastComplete = tracker.LastComplete()

        match thisSessionLastComplete with
        | Some position -> return Some position
        | None -> return! getPersistedPosition
    }

    member x.StartPersistingPosition () = 
        let writeUpdatedPosition position = async {
            let (complete, wait) = getPromise()

            let writeRequests =
                Write (
                    {
                        DocumentKey = positionDocumentKey
                        Document = position
                        Metadata = lazy(RavenOperations.emptyMetadata<EventPosition> documentStore)
                        Etag = null // just write this blindly
                    }, Guid.NewGuid())
                |> Seq.singleton

            do! writeQueue.Add(writeRequests, complete)

            let! success = wait 
            if(success) then
                lastPositionWritten <- Some position
            else
                ()
        }

        let rec loop () =  async {
            do! Async.Sleep(1000)

            let! position = x.LastComplete()

            do! 
                match (position, lastPositionWritten) with
                | Some position, None -> 
                    writeUpdatedPosition position
                | Some position, Some lastPosition
                    when position <> lastPosition ->
                    writeUpdatedPosition position
                | _ -> async { () }

            let! ct = Async.CancellationToken
            if(ct.IsCancellationRequested) then
                return ()
            else
                return! loop ()
        }
            
        let task = Async.StartAsTask(loop (), System.Threading.Tasks.TaskCreationOptions.None, cancellationToken)
        
        ()

    member x.DatabaseName = databaseName

    member x.Enqueue (message : 'TMessage) =
        async {
            match message.GlobalPosition with
            | Some position -> 
                do! tracker.Start position
            | None -> ()

            do! queue.Add message
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = 
        writeQueue.StartWork()
        queue.StartWork()