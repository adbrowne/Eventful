namespace Eventful.Tests.Integration

open Xunit
open System
open FSharpx.Collections
open FSharpx
open Raven.Client
open Eventful
open System.Runtime.Caching
type MyCountingDoc = Eventful.CsTests.MyCountingDoc

module Util = 
    let taskToAsync (task:System.Threading.Tasks.Task) =
        let wrappedTask = 
            task.ContinueWith(fun _ -> 
                if (task.IsFaulted) then
                    Some task.Exception
                else
                    None
            ) |> Async.AwaitTask
        async {
            let! result = wrappedTask
            match result with
            | Some e -> raise e
            | None -> return ()
        }

type BatchWrite = (string * MyCountingDoc * Raven.Abstractions.Data.Etag)

open Raven.Abstractions.Commands

type ProjectedDocument = (MyCountingDoc * Raven.Abstractions.Data.Etag)

open Raven.Json.Linq

module BatchOperations =
    let buildPutCommand (key, doc, etag) =
        let cmd = new PutCommandData()
        cmd.Document <- RavenJObject.FromObject(doc)
        cmd.Key <- key
        cmd.Etag <- etag

        let metadata = new Raven.Json.Linq.RavenJObject()
        metadata.Add("Raven-Entity-Name", new RavenJValue("MyCountingDoc"))
        cmd.Metadata <- metadata
        cmd
        
    let writeBatch (documentStore : Raven.Client.IDocumentStore) (docs:seq<BatchWrite>) = async {
        let! batchResult = 
            docs
            |> Seq.map buildPutCommand
            |> Seq.cast<ICommandData>
            |> Array.ofSeq
            |> documentStore.AsyncDatabaseCommands.BatchAsync
            |> Async.AwaitTask

        return (batchResult, docs)
    }

    
type BulkRavenProjector (documentStore:Raven.Client.IDocumentStore) =

    let serializer = Raven.Imports.Newtonsoft.Json.JsonSerializer.Create(new Raven.Imports.Newtonsoft.Json.JsonSerializerSettings())

    let cache = new MemoryCache("RavenBatchWrite")

    let writeBatch _ doc = async {
        let! (batchResult, docs) = BatchOperations.writeBatch documentStore doc
        let originalDocMap = 
            docs
            |> Seq.map (fun (key, doc, _) -> (key, doc))
            |> Map.ofSeq

        for docResult in batchResult do
            let doc = originalDocMap.[docResult.Key]
            cache.Set(docResult.Key, (doc, docResult.Etag) :> obj, DateTimeOffset.MaxValue) |> ignore
    }

    let writeQueue = new WorktrackingQueue<unit, BatchWrite>((fun _ -> Set.singleton ()), writeBatch, 10000, 10) 

    let grouping = fst >> Set.singleton

    let getDocument key (session : IAsyncDocumentSession) (cache : MemoryCache) =
        let cacheEntry = cache.Get(key)
        match cacheEntry with
        | :? ProjectedDocument as doc ->
            async { return Some doc }
        | _ when cacheEntry = null -> 
            async {
                let! doc = session.LoadAsync<MyCountingDoc>(key) |> Async.AwaitTask
                if (doc = null) then
                    return None
                else
                    let etag = session.Advanced.GetEtagFor(doc)
                    return Some (doc,etag)
            }
        | _ -> 
            // todo this exception does not bubble anywhere just silently crashes
            failwith <| sprintf "Unexpected type found for key %s found: %A expected: %A" key (cacheEntry.GetType()) typeof<ProjectedDocument>

    let getPromise () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let complete x = tcs.SetResult(x)
        (complete, Async.AwaitTask tcs.Task)
        
    let processEvent (key:Guid) values =
        async { 
            use session = documentStore.OpenAsyncSession()

            let docKey = "MyCountingDocs/" + key.ToString()

            let! existing = getDocument docKey session cache

            let buildNewDoc () =
                let newDoc = new MyCountingDoc()
                let etag = null
                (newDoc, etag)
                

            let (doc, etag) = existing |> Option.getOrElseF buildNewDoc

            let (complete, wait) = getPromise()
                
            doc.Writes <- doc.Writes + 1
            for (_, value) in values do
                let isEven = doc.Count % 2 = 0
                doc.Count <- doc.Count + 1
                if isEven then
                    doc.Value <- doc.Value + value
                else
                    doc.Value <- doc.Value - value
            do! writeQueue.AddWithCallback ((docKey, doc,etag), (fun _ -> async { complete true }))

            do! wait |> Async.Ignore
        }

    let queue = new WorktrackingQueue<Guid, Guid * int>(fst >> Set.singleton, processEvent, 10000, 10);

    member x.Enqueue key value =
       queue.Add (key,value)
   
    member x.WaitAll = queue.AsyncComplete

type RavenProjector (documentStore:Raven.Client.IDocumentStore) =

    let grouping = fst >> Set.singleton

    let processEvent (key:Guid) values =
        async { 
            use session = documentStore.OpenAsyncSession()

            let docKey = "MyCountingDocs/" + key.ToString()
            let! doc = session.LoadAsync<MyCountingDoc>(docKey) |> Async.AwaitTask
            let! doc = async { 
                if doc = null then
                    let newDoc = new MyCountingDoc()
                    do! session.StoreAsync(newDoc, docKey) |> Util.taskToAsync
                    return newDoc
                else
                    return doc
            }

            for (_, value) in values do
                let isEven = doc.Count % 2 = 0
                doc.Count <- doc.Count + 1
                if isEven then
                    doc.Value <- doc.Value + value
                else
                    doc.Value <- doc.Value - value
            do! session.SaveChangesAsync() |> Util.taskToAsync
        }

    let queue = new WorktrackingQueue<Guid, Guid * int>(fst >> Set.singleton, processEvent, 10000, 10);

    member x.Enqueue key value =
       queue.Add (key,value)
   
    member x.WaitAll = queue.AsyncComplete

module RavenProjectorTests = 

    let buildDocumentStore () =
        let documentStore = new Raven.Client.Document.DocumentStore()
        documentStore.Url <- "http://localhost:8080"
        documentStore.DefaultDatabase <- "tenancy-blue"
        documentStore.Initialize() |> ignore
        documentStore

    [<Fact>]
    let ``Test Bulk Write`` () : unit =
        let documentStore = buildDocumentStore()
        let projector = new BulkRavenProjector(documentStore :> Raven.Client.IDocumentStore)

        let docKey = "MyCountingDocs/" + Guid.NewGuid().ToString()

        seq {
            yield (docKey, new MyCountingDoc(), null)
        }
        |> BatchOperations.writeBatch documentStore 
        |> Async.Ignore
        |> Async.RunSynchronously

    [<Fact>]
    let ``Pump many events at Raven`` () : unit =
        let documentStore = buildDocumentStore()

        let projector = new BulkRavenProjector(documentStore :> Raven.Client.IDocumentStore)

        let values = [1..100]
        let streams = [for i in 1 .. 1000 -> Guid.NewGuid()]

        let streamValues = 
            streams
            |> Seq.map (fun x -> (x,values))
            |> Map.ofSeq

        let rnd = new Random(1024)

        let rec generateStream (remainingStreams, remainingValues:Map<Guid, int list>) = 
            match remainingStreams with
            | [] -> None
            | _ ->
                let index = rnd.Next(0, remainingStreams.Length - 1)
                let blah = List.nth
                let key =  List.nth remainingStreams index
                let values = remainingValues |> Map.find key

                match values with
                | [] -> failwith ("Empty sequence should not happen")
                | [x] -> 
                    let beforeIndex = remainingStreams |> List.take index
                    let afterIndex = remainingStreams |> List.skip (index + 1) 
                    let remainingStreams' = (beforeIndex @ afterIndex)
                    let remainingValues' = (remainingValues |> Map.remove key)
                    let nextValue = (key,x)
                    let remaining = (remainingStreams', remainingValues')
                    Some (nextValue, remaining)
                | x::xs ->
                    let remainingValues' = (remainingValues |> Map.add key xs)
                    let nextValue = (key,x)
                    let remaining = (remainingStreams, remainingValues')
                    Some (nextValue, remaining)

        let myEvents = (streams, streamValues) |> Seq.unfold generateStream

        async {
            for (key,value) in myEvents do
                do! projector.Enqueue key value
            do! projector.WaitAll()
        } |> Async.RunSynchronously 

        ()