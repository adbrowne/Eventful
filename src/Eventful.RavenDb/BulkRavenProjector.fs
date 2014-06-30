namespace Eventful.Raven

open System
open System.Runtime.Caching

open Eventful

open FSharpx

open Raven.Client
open Raven.Abstractions.Data
open Raven.Json.Linq

type HashSet<'T> = System.Collections.Generic.HashSet<'T>

[<CustomEquality; CustomComparison>]
type UntypedDocumentProcessor<'TContext> = {
    ProcessorKey : string
    Process : IDocumentFetcher -> obj -> seq<SubscriberEvent<'TContext>> -> Async<seq<DocumentWriteRequest>>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<IComparable>
    EventTypes : HashSet<Type>
}
with
    static member Key p = 
        let {ProcessorKey = key } = p
        key
    override x.Equals(y) = 
        equalsOn UntypedDocumentProcessor<'TContext>.Key x y
    override x.GetHashCode() = 
        hashOn UntypedDocumentProcessor<'TContext>.Key x
    interface System.IComparable with 
        member x.CompareTo y = compareOn UntypedDocumentProcessor<'TContext>.Key x y

module RavenOperations =
    let getDocument (documentStore : Raven.Client.IDocumentStore) (cache : MemoryCache) database docKey =
        let cacheEntry = cache.Get(database + "::" + docKey)
        match cacheEntry with
        | :? ProjectedDocument<_> as doc ->
            async { return Some doc }
        | _ -> 
            async {
                use session = documentStore.OpenAsyncSession(database)
                let! doc = session.LoadAsync<_>(docKey) |> Async.AwaitTask
                if Object.Equals(doc, null) then
                    return None
                else
                    let etag = session.Advanced.GetEtagFor(doc)
                    let metadata = session.Advanced.GetMetadataFor(doc)
                    return Some (doc, metadata, etag)
            }

    let emptyMetadata (entityName : string) = 
        let metadata = new Raven.Json.Linq.RavenJObject()
        metadata.Add("Raven-Entity-Name", new RavenJValue(entityName))
        metadata

type ProcessorSet<'TEventContext>(processors : List<UntypedDocumentProcessor<'TEventContext>>) =
    member x.Items = processors
    member x.Add<'TKey,'TDocument>(processor:DocumentProcessor<'TKey, 'TDocument, 'TEventContext>) =
        
        let processUntyped (fetcher:IDocumentFetcher) (untypedKey : obj) events =
            let key = untypedKey :?> 'TKey
            processor.Process key fetcher events

        let matchingKeysUntyped event =
            processor.MatchingKeys event
            |> Seq.cast<IComparable>

        let untypedProcessor = {
            ProcessorKey = "ProcessorFor: " + typeof<'TDocument>.FullName
            Process = processUntyped
            MatchingKeys = matchingKeysUntyped
            EventTypes = new HashSet<Type>(processor.EventTypes)
        }

        let processors' = untypedProcessor::processors
        new ProcessorSet<'TEventContext>(processors')

    static member Empty = new ProcessorSet<'TEventContext>(List.empty)

type BulkRavenProjector<'TEventContext> 
    (
        documentStore:Raven.Client.IDocumentStore, 
        processors:ProcessorSet<'TEventContext>,
        databaseName: string,
        getPosition:'TEventContext -> EventPosition,
        maxQueueSize : int,
        workers: int
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

    let writeQueue = new WorktrackingQueue<unit, BatchWrite>((fun _ -> Set.singleton ()), writeBatch, maxQueueSize, workers) 

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

    let grouper event =
        processors.Items
        |> Seq.collect (fun x -> 
            if x.EventTypes.Contains(event.GetType()) then
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
        let x = new WorktrackingQueue<_,_>(grouper, processEvent, 200000, 10, eventComplete);
        x.StopWork()
        x

    member x.LastComplete = tracker.LastComplete

    member x.Enqueue subscriberEvent =
        async {
            do! subscriberEvent.Context |> getPosition |> tracker.Start 
            do! queue.Add subscriberEvent
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = queue.StartWork()