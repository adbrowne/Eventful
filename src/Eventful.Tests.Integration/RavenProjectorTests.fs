﻿namespace Eventful.Tests.Integration

open Xunit
open System
open FSharpx.Collections
open FSharpx
open FsUnit.Xunit
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

open Raven.Abstractions.Data

type DocumentWriteRequest = {
    DocumentKey : string
    Document : Lazy<Raven.Json.Linq.RavenJObject>
    Metadata : Lazy<Raven.Json.Linq.RavenJObject>
    Etag : Raven.Abstractions.Data.Etag
}

type BatchWrite = (seq<DocumentWriteRequest> * (bool -> Async<unit>))

open Raven.Abstractions.Commands

type ProjectedDocument = (MyCountingDoc * Raven.Json.Linq.RavenJObject * Raven.Abstractions.Data.Etag)

open Raven.Json.Linq

type objToObj = obj -> obj

type objToSeqObjToObj = obj -> seq<objToObj>

type UntypedDocumentProcessor = {
    EventTypes : seq<Type>
    Match: (obj -> seq<obj -> seq<objToSeqObjToObj>>)
    NewDocument: obj -> obj
}

type DocumentHandler<'TDocument> = ('TDocument -> 'TDocument)

type DocumentHandlerSet<'TKey, 'TDocument> = seq<('TKey * seq<DocumentHandler<'TDocument>>)>

type SubscriberEvent<'TContext> = {
    Event : obj
    Context : 'TContext
    StreamId : string
    EventNumber: int
}

[<CustomComparison>]
[<CustomEquality>]
type DocumentProcessor<'TKey, 'TDocument, 'TContext> = {
    ProcessorKey : string
    GetDocumentKey : 'TKey -> string
    EventTypes : seq<Type>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<'TKey>
    Process: 'TKey -> 'TDocument -> SubscriberEvent<'TContext> -> 'TDocument
    NewDocument : 'TKey -> 'TDocument
}
with
    override x.Equals(y) = (match y with :? DocumentProcessor<'TKey, 'TDocument, 'TContext> as y -> x.ProcessorKey = y.ProcessorKey | _ -> false)
    override x.GetHashCode() = x.ProcessorKey.GetHashCode()
    interface System.IComparable with 
        member x.CompareTo y = x.ProcessorKey.CompareTo (y :?> DocumentProcessor<'TKey, 'TDocument, 'TContext>).ProcessorKey

//with 
//    static member ToUntypedDocumentProcessor (x:DocumentProcessor<'TKey, 'TDocument>) =
//        let matchWithConversion (eventObj : obj) =
//            x.Match (eventObj)
//
//        let newDocumentWithConversion (key : obj) = (x.NewDocument (key :?> 'TKey)) :> obj
//
//        {
//            UntypedDocumentProcessor.EventTypes = x.EventTypes
//            Match = matchWithConversion
//            NewDocument = newDocumentWithConversion
//        }

module BatchOperations =
    let buildPutCommand (writeRequest:DocumentWriteRequest) =
        let cmd = new PutCommandData()
        cmd.Document <- writeRequest.Document.Force()
        cmd.Key <- writeRequest.DocumentKey
        cmd.Etag <- writeRequest.Etag

        cmd.Metadata <- writeRequest.Metadata.Force()
        cmd
        
    let writeBatch (documentStore : Raven.Client.IDocumentStore) (docs:seq<BatchWrite>) = async {
        try 
            let! batchResult = 
                docs
                |> Seq.collect (fst >> Seq.map buildPutCommand)
                |> Seq.cast<ICommandData>
                |> Array.ofSeq
                |> documentStore.AsyncDatabaseCommands.BatchAsync
                |> Async.AwaitTask

            return Some (batchResult, docs)
        with | e -> return None
    }

type CurrentDoc = (MyCountingDoc * RavenJObject * Etag)

type EventContext = {
    Tenancy : string
}

type BulkRavenProjector<'TEventContext> 
    (
        documentStore:Raven.Client.IDocumentStore, 
        processors:seq<DocumentProcessor<Guid, CurrentDoc, 'TEventContext>>
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

    let getDocument key (documentStore : Raven.Client.IDocumentStore) (cache : MemoryCache) =
        let cacheEntry = cache.Get(key)
        match cacheEntry with
        | :? ProjectedDocument as doc ->
            async { return Some doc }
        | _ -> 
            async {
                use session = documentStore.OpenAsyncSession()
                let! doc = session.LoadAsync<MyCountingDoc>(key) |> Async.AwaitTask
                if (doc = null) then
                    return None
                else
                    let etag = session.Advanced.GetEtagFor(doc)
                    let metadata = session.Advanced.GetMetadataFor(doc)
                    return Some (doc, metadata, etag)
            }

    let getPromise () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let complete  = fun success -> async { tcs.SetResult(success) }
        (complete, Async.AwaitTask tcs.Task)
        
    let tryEvent (key : Guid, documentProcessor : DocumentProcessor<Guid,CurrentDoc, 'TEventContext>) events =
        async { 
            let docKey = documentProcessor.GetDocumentKey(key)
            
            let buildNewDoc () =
                documentProcessor.NewDocument key

            let! (doc, metadata, etag) = 
                getDocument docKey documentStore cache
                |> Async.map (Option.getOrElseF buildNewDoc)

            doc.Writes <- doc.Writes + 1

            let (doc, metadata, etag) = 
                events
                |> Seq.fold (documentProcessor.Process key) (doc, metadata, etag)

            let (complete, wait) = getPromise()
                
            let writeRequests =
                {
                    DocumentKey = docKey
                    Document = lazy(RavenJObject.FromObject(doc))
                    Metadata = lazy(metadata)
                    Etag = etag
                }
                |> Seq.singleton

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
        processors
        |> Seq.collect (fun x -> 
            x.MatchingKeys event
            |> Seq.map (fun k9 -> (k9, x)))
        |> Set.ofSeq

    let queue = new WorktrackingQueue<(Guid * DocumentProcessor<Guid,CurrentDoc,_>), SubscriberEvent<'TEventContext>>(grouper, processEvent, 10000, 10);

    member x.Enqueue subscriberEvent =
       queue.Add subscriberEvent
   
    member x.WaitAll = queue.AsyncComplete

module RavenProjectorTests = 

    let buildDocumentStore () =
        let documentStore = new Raven.Client.Document.DocumentStore()
        documentStore.Url <- "http://localhost:8080"
        documentStore.DefaultDatabase <- "tenancy-blue"
        documentStore.Initialize() |> ignore
        documentStore

//    [<Fact>]
//    let ``Test Bulk Write`` () : unit =
//        let documentStore = buildDocumentStore()
//        let projector = new BulkRavenProjector(documentStore :> Raven.Client.IDocumentStore)
//
//        let docKey = "MyCountingDoc/" + Guid.NewGuid().ToString()
//
//        let result = 
//            seq {
//                let doc = new MyCountingDoc()
//                let metadata = new Raven.Json.Linq.RavenJObject()
//                metadata.Add("Raven-Entity-Name", new RavenJValue("MyCountingDocs"))
//                let writeRequests = Seq.singleton {
//                    DocumentKey = docKey
//                    Document = lazy(RavenJObject.FromObject(doc))
//                    Metadata = lazy(metadata)
//                    Etag = Raven.Abstractions.Data.Etag.Empty }
//                yield (writeRequests, (fun _ -> async { () }))
//            }
//            |> BatchOperations.writeBatch documentStore 
//            |> Async.RunSynchronously
//
//        match result with
//        | Some _ -> (true |> should equal true)
//        | None -> (false |> should equal true)

    [<Fact>]
    let ``Pump many events at Raven`` () : unit =
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

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

        let myEvents = 
            (streams, streamValues) 
            |> Seq.unfold generateStream
            |> Seq.map (fun (key, value) ->
                {
                    Event = (key, value)
                    Context = { Tenancy = "tenancy-blue" }
                    StreamId = key.ToString()
                    EventNumber = 0
                }
            )

        let processValue (value:int) (docObj:CurrentDoc) =
            let (doc, metadata, etag) = docObj
            let isEven = doc.Count % 2 = 0
            doc.Count <- doc.Count + 1
            if isEven then
                doc.Value <- doc.Value + value
            else
                doc.Value <- doc.Value - value

            (doc, metadata, etag)

        let buildNewDoc (id : Guid) =
            let newDoc = new MyCountingDoc()
            newDoc.Id <- id
            let etag = Raven.Abstractions.Data.Etag.Empty

            let metadata = new Raven.Json.Linq.RavenJObject()
            metadata.Add("Raven-Entity-Name", new RavenJValue("MyCountingDocs"))

            (newDoc, metadata, etag)

        let matcher (subscriberEvent : SubscriberEvent<EventContext>) =
            match subscriberEvent.Event with
            | :? (Guid * int) as event ->
                event |> fst |> Seq.singleton
            | _ -> Seq.empty

        let processEvent key doc subscriberEvent = 
            match subscriberEvent.Event with
            | :? (Guid * int) as event ->
                let (key, value) = event
                processValue value doc
            | _ -> doc

        let processors : seq<DocumentProcessor<Guid,CurrentDoc, EventContext>> = seq {
            yield {
                ProcessorKey = "Mine"
                GetDocumentKey = (fun (key:Guid) -> "MyCountingDocs/" + key.ToString())
                EventTypes = Seq.singleton typeof<int>
                MatchingKeys = matcher
                Process = processEvent
                NewDocument = buildNewDoc
            }
        }

        let projector = new BulkRavenProjector<EventContext>(documentStore, processors)

        seq {
            yield async {
                for event in myEvents do
                    do! projector.Enqueue event
                do! projector.WaitAll()
            }

            yield! seq {
                for key in streams do
                    yield (fun () -> async {
                        use session = documentStore.OpenAsyncSession()
                        let docKey = "MyCountingDocs/" + (key.ToString())
                        let! doc = session.LoadAsync<MyCountingDoc>(docKey) |> Async.AwaitTask

                        let! doc = 
                            if (doc = null) then 
                                let newDoc = new MyCountingDoc()
                                newDoc.Id <- key
                                async {
                                    do! session.StoreAsync(newDoc :> obj, docKey) |> Util.taskToAsync
                                    return newDoc
                                }
                            else async { return doc }
                        doc.Foo <- "Bar"
                        try
                            do! session.SaveChangesAsync() |> Util.taskToAsync
                        with 
                            | e -> printfn "Failed: %A" docKey
                                   raise e
                    }) |> runAsyncUntilSuccess
            }
        }
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        async {
            use session = documentStore.OpenAsyncSession()

            let! docs = session.Advanced.LoadStartingWithAsync<MyCountingDoc>("MyCountingDocs/", 0, 1024) |> Async.AwaitTask
            for doc in docs do
                doc.Count |> should equal 100
                doc.Foo |> should equal "Bar"
                doc.Value |> should equal -50
            ()
        } |> Async.RunSynchronously
        ()