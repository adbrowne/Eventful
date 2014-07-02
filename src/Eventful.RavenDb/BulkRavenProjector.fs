namespace Eventful.Raven

open System
open System.Runtime.Caching

open Eventful
open Metrics

open FSharpx

open Raven.Client
open Raven.Abstractions.Data
open Raven.Json.Linq

type BulkRavenProjector<'TEventContext> 
    (
        documentStore:Raven.Client.IDocumentStore, 
        processors:ProcessorSet<'TEventContext>,
        databaseName: string,
        getPosition:'TEventContext -> EventPosition,
        maxEventQueueSize : int,
        eventWorkers: int,
        maxWriterQueueSize: int,
        writerWorkers: int
    ) =

    let cache = new MemoryCache("RavenBatchWrite-" + databaseName)

    let batchWriteTracker = Metric.Histogram("BatchWriteSize", Unit.Items)
    let completeItemsTracker = Metric.Meter("EventsComplete", Unit.Items)
    let processingExceptions = Metric.Meter("ProcessingExceptions", Unit.Items)
    let batchWriteTime = Metric.Timer("WriteTime", Unit.None)
    let batchWritesMeter = Metric.Meter("BatchWrites", Unit.Items)
    let writeBatch _ docs = async {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let originalDocMap = 
            docs
            |> Seq.collect (fun (writeRequests, callback) -> 
                writeRequests
                |> Seq.map(fun { DocumentKey = key; Document = document } ->
                    let document = document.Force()
                    (key, (document, callback))
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
                    let (doc, callback) = originalDocMap.[docResult.Key]
                    cache.Set(docResult.Key, (doc, docResult.Etag) :> obj, DateTimeOffset.MaxValue) |> ignore
                true
            | None ->
                for (docKey, (_, callback)) in originalDocMap |> Map.toSeq do
                    cache.Remove(docKey) |> ignore
                false
        
        for (_, callback) in docs do
            do! callback writeSuccessful

        sw.Stop()
        batchWriteTime.Record(sw.ElapsedMilliseconds, TimeUnit.Milliseconds)
    }

    let writeQueue = new WorktrackingQueue<unit, BatchWrite, BatchWrite>((fun a -> (a, Set.singleton ())), writeBatch, maxWriterQueueSize, writerWorkers, name = databaseName + " write") 

    let getPromise () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let complete  = fun success -> async { tcs.SetResult(success) }
        (complete, Async.AwaitTask tcs.Task)
        
    let fetcher = {
        new IDocumentFetcher with
            member x.GetDocument key =
                RavenOperations.getDocument documentStore cache databaseName key
    }

    let tryEvent (key : IComparable, documentProcessor : UntypedDocumentProcessor<'TEventContext>) events =
        async { 
            let untypedKey = key :> obj

            let! writeRequests = documentProcessor.Process fetcher untypedKey events
            let (complete, wait) = getPromise()
                
            do! writeQueue.Add(writeRequests, complete)

            return! wait 
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
                    return! loop(count + 1)
            else
                processingExceptions.Mark()
                consoleLog <| sprintf "Processing failed permanently: %A %A" key values
                ()
        }
        do! loop 0
    }

    let grouper (event : SubscriberEvent<'TEventContext>) =
        let groups = 
            processors.Items
            |> Seq.collect (fun x -> 
                if x.EventTypes.Contains(event.Event.GetType()) then
                    x.MatchingKeys event
                    |> Seq.map (fun k9 -> (k9, x))
                else 
                    Seq.empty)
            |> Set.ofSeq
        (event, groups)
    
    let tracker = new LastCompleteItemAgent2<EventPosition>()

    let eventComplete (event:SubscriberEvent<'TEventContext>) =
        let position = getPosition event.Context
        async {
            tracker.Complete(position)
            completeItemsTracker.Mark(1L)
        }

    let queue = 
        let x = new WorktrackingQueue<_,_,_>(grouper, processEvent, maxEventQueueSize, eventWorkers, eventComplete);
        x.StopWork()
        x

    let positionDocumentKey = "EventProcessingPosition"

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
        let rec loop () =  async {
            do! Async.Sleep(1000)

            let (complete, wait) = getPromise()

            let! position = x.LastComplete()

            do! 
                match position with
                | Some position -> async {
                        let writeRequests =
                            {
                                DocumentKey = positionDocumentKey
                                Document = lazy(RavenOperations.serializeDocument documentStore position)
                                Metadata = lazy(RavenOperations.emptyMetadata<EventPosition> documentStore)
                                Etag = null // just write this blindly
                            }   
                            |> Seq.singleton

                        do! writeQueue.Add(writeRequests, complete)

                        do! wait |> Async.Ignore
                    }
                | None -> async { () }

            return! loop()
        }
            
        loop () |> Async.StartAsTask |> ignore
        ()

    member x.DatabaseName = databaseName

    member x.Enqueue subscriberEvent =
        async {
            do! subscriberEvent.Context |> getPosition |> tracker.Start 
            do! queue.Add subscriberEvent
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = queue.StartWork()