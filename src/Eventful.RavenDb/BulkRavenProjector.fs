namespace Eventful.Raven

open System
open System.Runtime.Caching

open Eventful

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

    let cache = new MemoryCache("RavenBatchWrite")

    let writeBatch _ docs = async {
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
    }

    let writeQueue = new WorktrackingQueue<unit, BatchWrite>((fun _ -> Set.singleton ()), writeBatch, maxWriterQueueSize, writerWorkers) 

    let getPromise () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let complete  = fun success -> async { tcs.SetResult(success) }
        (complete, Async.AwaitTask tcs.Task)
        
    let tryEvent (key : IComparable, documentProcessor : UntypedDocumentProcessor<'TEventContext>) events =
        async { 
            let untypedKey = key :> obj

            let fetcher = {
                new IDocumentFetcher with
                    member x.GetDocument key =
                        RavenOperations.getDocument documentStore cache databaseName key
            }

            let! writeRequests = documentProcessor.Process fetcher untypedKey events
            let (complete, wait) = getPromise()
                
            do! writeQueue.Add(writeRequests, complete)

            return! wait 
        }
        
    let processEvent key values = async {
        let rec loop () = async {
            let! attempt = tryEvent key values
            if not attempt then
                return! loop ()
            else
                ()
        }
        do! loop ()
    }

    let grouper (event : SubscriberEvent<'TEventContext>) =
        processors.Items
        |> Seq.collect (fun x -> 
            if x.EventTypes.Contains(event.Event.GetType()) then
                x.MatchingKeys event
                |> Seq.map (fun k9 -> (k9, x))
            else 
                Seq.empty)
        |> Set.ofSeq
    
    let tracker = new LastCompleteItemAgent2<EventPosition>()

    let eventComplete (event:SubscriberEvent<'TEventContext>) =
        let position = getPosition event.Context
        async {
            tracker.Complete(position)
        }

    let queue = 
        let x = new WorktrackingQueue<_,_>(grouper, processEvent, maxEventQueueSize, eventWorkers, eventComplete);
        x.StopWork()
        x

    member x.LastComplete = tracker.LastComplete

    member x.DatabaseName = databaseName

    member x.Enqueue subscriberEvent =
        async {
            do! subscriberEvent.Context |> getPosition |> tracker.Start 
            do! queue.Add subscriberEvent
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = queue.StartWork()