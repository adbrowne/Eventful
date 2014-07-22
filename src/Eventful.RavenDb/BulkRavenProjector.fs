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
        readQueue : RavenReadQueue
    ) =
    let log = Common.Logging.LogManager.GetLogger(typeof<BulkRavenProjector<_>>)

    let completeItemsTracker = Metric.Meter(sprintf "EventsComplete %s" databaseName, Unit.Items)
    let processingExceptions = Metric.Meter(sprintf "ProcessingExceptions %s" databaseName, Unit.Items)

    let fetcher = {
        new IDocumentFetcher with
            member x.GetDocument<'TDocument> key = async {
                //runWithTimeout "Single Fetcher" (TimeSpan.FromSeconds(30.0)) <| RavenOperations.getDocument<'TDocument> documentStore cache databaseName key
                let! result = readQueue.Work databaseName <| Seq.singleton (key, typeof<'TDocument>)
                let (key, t, result) = Seq.head result

                match result with
                | Some (doc, metadata, etag) -> 
                    return (Some (doc :?> 'TDocument, metadata, etag))
                | None -> 
                    return None
            }
            member x.GetDocuments request = async {
                return! readQueue.Work databaseName request
            } 
                //runWithTimeout "Multi Fetcher" (TimeSpan.FromSeconds(30.0)) <| RavenOperations.getDocuments documentStore cache databaseName request
    }

    let positionDocumentKey = "EventProcessingPosition"

    let getPersistedPosition = async {
        let! (persistedLastComplete : ProjectedDocument<EventPosition> option) = fetcher.GetDocument positionDocumentKey
        return persistedLastComplete |> Option.map((fun (doc,_,_) -> doc))
    }

    let tryEvent (key : string) events =
        async { 
            let! writeRequests = documentProcessor.Process key fetcher events

            return! writeQueue.Work databaseName writeRequests
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
                    //consoleLog <| sprintf "Exception while processing: %A %A %A %A" e e.StackTrace key values
                    return! loop(count + 1)
            else
                processingExceptions.Mark()
                consoleLog <| sprintf "Processing failed permanently: %A %A" key values
                ()
        }
        do! loop 0
    }

    let grouper (event : 'TMessage) =
        let docIds = 
            documentProcessor.MatchingKeys event
            |> Set.ofSeq

        (event, docIds)
    
    let tracker = 
        let t = new LastCompleteItemAgent<EventPosition>(name = databaseName)
//        async {
//            let! persistedPosition = getPersistedPosition
//
//            match persistedPosition with
//            | Some pos ->
//                do! t.Start pos
//                t.Complete pos
//            | None -> ()
//                
//        } |> Async.RunSynchronously

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
        let q = new WorktrackingQueue<_,_,_>(grouper, processEvent, maxEventQueueSize, eventWorkers, eventComplete, name = databaseName + " processing", cancellationToken = cancellationToken);
        q.StopWork()
        q

    let mutable lastPositionWritten : Option<EventPosition> = None

    member x.LastComplete () = tracker.LastComplete()

    member x.StartPersistingPosition () = 
        let writeUpdatedPosition position = async {
            let writeRequests =
                Write (
                    {
                        DocumentKey = positionDocumentKey
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
                do! tracker.Start position
            | None -> ()

            do! queue.Add message
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = 
        // writeQueue.StartWork()
        queue.StartWork()