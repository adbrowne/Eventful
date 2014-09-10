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
        documentProcessor:DocumentProcessor<string, 'TMessage>,
        databaseName: string,
        maxEventQueueSize : int,
        eventWorkers: int,
        onEventComplete : 'TMessage -> Async<unit>,
        cancellationToken : CancellationToken,
        writeQueue : RavenWriteQueue,
        readQueue : RavenReadQueue,
        workTimeout : TimeSpan option
    ) =
    let log = createLogger "Eventful.Raven.BulkRavenProjector"

    let completeItemsTracker = Metric.Meter(sprintf "EventsComplete %s" databaseName, Unit.Items)
    let processingExceptions = Metric.Meter(sprintf "ProcessingExceptions %s" databaseName, Unit.Items)

    let fetcher = new DocumentFetcher(documentStore, databaseName, readQueue) :> IDocumentFetcher

    let getPersistedPosition = async {
        let! (persistedLastComplete : ProjectedDocument<EventPosition> option) = fetcher.GetDocument RavenConstants.PositionDocumentKey |> Async.AwaitTask
        return persistedLastComplete |> Option.map((fun (doc,_,_) -> doc))
    }

    let tryEvent (key : string) events =
        async { 
            let! writeRequests = documentProcessor.Process(key, fetcher, events).Invoke() |> Async.AwaitTask

            return! writeQueue.Work databaseName writeRequests
        }
        
    let processEvent key values = async {
        let cachedValues = values |> Seq.cache
        let maxAttempts = 10
        let rec loop count ex = async {
            if count < maxAttempts then
                try
                    let! attempt = tryEvent key cachedValues
                    if not attempt then
                        return! loop (count + 1) None
                    else
                        ()
                with | ex ->
                    //consoleLog <| sprintf "Exception while processing: %A %A %A %A" e e.StackTrace key values
                    return! loop(count + 1) (Some ex)
            else
                processingExceptions.Mark()
                match ex with
                | Some ex ->
                    log.ErrorWithException <| lazy(sprintf "Processing failed permanently for %A" key, ex)
                | None -> 
                    log.Error <| lazy(sprintf "Processing failed permanently - no exception key: %A" key)
                ()
        }
        do! loop 0 None
    }

    let grouper (event : 'TMessage) =
        let docIds = 
            documentProcessor.MatchingKeys event
            |> Set.ofSeq

        (event, docIds)
    
    let tracker = 
        let t = new LastCompleteItemAgent<EventPosition>(name = databaseName)

        async {
            let! persistedPosition = getPersistedPosition

            let position = 
                persistedPosition |> Option.getOrElse EventPosition.Start

            t.Start position
            t.Complete position
                
        } |> Async.RunSynchronously

        t

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
        new WorktrackingQueue<_,_,_>(
            grouper, 
            processEvent, 
            maxEventQueueSize, 
            eventWorkers, 
            eventComplete, 
            name = databaseName + " processing", 
            cancellationToken = cancellationToken, 
            groupComparer = StringComparer.InvariantCultureIgnoreCase, 
            runImmediately = false,
            workTimeout = workTimeout)

    let mutable lastPositionWritten : Option<EventPosition> = None

    /// fired each time a full queue is detected
    [<CLIEvent>]
    member this.QueueFullEvent = queue.QueueFullEvent

    member x.LastComplete () = tracker.LastComplete()

    member x.StartPersistingPosition () = 
        let writeUpdatedPosition position = async {
            let writeRequests =
                Write (
                    {
                        DocumentKey = RavenConstants.PositionDocumentKey
                        Document = position
                        Metadata = lazy(RavenOperations.emptyMetadata<EventPosition> documentStore)
                        Etag = null // just write this blindly
                    }, Guid.NewGuid())
                |> Seq.singleton

            let! success = writeQueue.Work databaseName writeRequests

            if(success) then
                lastPositionWritten <- Some position
            else
                ()
        }

        let rec loop () =  async {
            do! Async.Sleep(5000)

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
            
        let taskName = sprintf "Persist Position %s" databaseName
        let task = runAsyncAsTask taskName cancellationToken <| loop ()
        
        ()

    member x.DatabaseName = databaseName

    member x.Enqueue (message : 'TMessage) =
        async {
            match message.GlobalPosition with
            | Some position -> 
                tracker.Start position
            | None -> ()

            do! queue.Add message
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = 
        // writeQueue.StartWork()
        queue.StartWork()