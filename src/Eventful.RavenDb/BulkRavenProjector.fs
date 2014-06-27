namespace Eventful.Raven

open System
open System.Runtime.Caching

open Eventful

open FSharpx

open Raven.Client
open Raven.Abstractions.Data
open Raven.Json.Linq

type ProjectedDocument<'TDocument> = ('TDocument * Raven.Json.Linq.RavenJObject * Raven.Abstractions.Data.Etag)

[<CustomEquality; CustomComparison>]
type UntypedDocumentProcessor<'TContext> = {
    ProcessorKey : string
    Process : Raven.Client.IDocumentStore -> MemoryCache -> obj -> seq<SubscriberEvent<'TContext>> -> Async<seq<DocumentWriteRequest>>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<IComparable>
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
    let getDocument<'TDocument> (documentStore : Raven.Client.IDocumentStore) (cache : MemoryCache) docKey =
        let cacheEntry = cache.Get(docKey)
        match cacheEntry with
        | :? ProjectedDocument<'TDocument> as doc ->
            async { return Some doc }
        | _ -> 
            async {
                use session = documentStore.OpenAsyncSession()
                let! doc = session.LoadAsync<'TDocument>(docKey) |> Async.AwaitTask
                if Object.Equals(doc, null) then
                    return None
                else
                    let etag = session.Advanced.GetEtagFor(doc)
                    let metadata = session.Advanced.GetMetadataFor(doc)
                    return Some (doc, metadata, etag)
            }

    let toUntypedProjectedDocument (document, metadata, etag) =
        (document :> obj, metadata, etag)

type ProcessorSet<'TEventContext>(processors : List<UntypedDocumentProcessor<'TEventContext>>) =
    member x.Items = processors
    member x.Add<'TKey,'TDocument>(processor:DocumentProcessor<'TKey, 'TDocument, 'TEventContext>) =
        
        let processUntyped store cache (untypedKey : obj) events =
            let key = untypedKey :?> 'TKey
            let docKey = processor.GetDocumentKey key
            async {
                let! fetch = RavenOperations.getDocument<'TDocument> store cache docKey
                let (doc, metadata, etag) =  
                    fetch 
                    |> Option.getOrElseF (fun () -> processor.NewDocument key)

                let (doc, metadata, etag) = 
                    events
                    |> Seq.fold (processor.Process key) (doc, metadata, etag)

                let (doc, metadata, etag) = processor.BeforeWrite (doc, metadata, etag)

                return 
                    {
                        DocumentKey = docKey
                        Document = lazy(RavenJObject.FromObject(doc))
                        Metadata = lazy(metadata)
                        Etag = etag
                    }
                    |> Seq.singleton
            }

        let matchingKeysUntyped event =
            processor.MatchingKeys event
            |> Seq.cast<IComparable>

        let untypedProcessor = {
            ProcessorKey = "ProcessorFor: " + typeof<'TDocument>.FullName
            Process = processUntyped
            MatchingKeys = matchingKeysUntyped
        }

        new ProcessorSet<'TEventContext>(untypedProcessor::processors)

    static member Empty = new ProcessorSet<'TEventContext>(List.empty)

type BulkRavenProjector<'TEventContext> 
    (
        documentStore:Raven.Client.IDocumentStore, 
        processors:ProcessorSet<'TEventContext>
    ) =

    let serializer = Raven.Imports.Newtonsoft.Json.JsonSerializer.Create(new Raven.Imports.Newtonsoft.Json.JsonSerializerSettings())

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

        let! result = BatchOperations.writeBatch documentStore docs
        match result with
        | Some (batchResult, docs) ->
            for docResult in batchResult do
                let (doc, callback) = originalDocMap.[docResult.Key]
                cache.Set(docResult.Key, (doc, docResult.Etag) :> obj, DateTimeOffset.MaxValue) |> ignore
                do! callback true
        | None ->
            for (docKey, (_, callback)) in originalDocMap |> Map.toSeq do
                cache.Remove(docKey) |> ignore
                do! callback false
    }

    let writeQueue = new WorktrackingQueue<unit, BatchWrite>((fun _ -> Set.singleton ()), writeBatch, 10000, 10) 

    let getPromise () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let complete  = fun success -> async { tcs.SetResult(success) }
        (complete, Async.AwaitTask tcs.Task)
        
    let tryEvent (key : IComparable, documentProcessor : UntypedDocumentProcessor<'TEventContext>) events =
        async { 
            let untypedKey = key :> obj

            // doc.Writes <- doc.Writes + 1

            let! writeRequests = documentProcessor.Process documentStore cache untypedKey events
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
            x.MatchingKeys event
            |> Seq.map (fun k9 -> (k9, x)))
        |> Set.ofSeq

    let queue = new WorktrackingQueue<(IComparable * UntypedDocumentProcessor<_>), SubscriberEvent<'TEventContext>>(grouper, processEvent, 10000, 10);

    member x.Enqueue subscriberEvent =
       queue.Add subscriberEvent
   
    member x.WaitAll = queue.AsyncComplete
