namespace Eventful.Tests.Integration

open Xunit
open System
open FSharpx.Collections
open FSharpx
open FsUnit.Xunit
open Raven.Client
open Eventful
open Eventful.Raven
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

open Raven.Abstractions.Commands

open Raven.Json.Linq

type objToObj = obj -> obj

type objToSeqObjToObj = obj -> seq<objToObj>

type UntypedDocumentProcessor = {
    EventTypes : seq<Type>
    Match: (obj -> seq<obj -> seq<objToSeqObjToObj>>)
    NewDocument: obj -> obj
}

[<CLIMutable>]
type MyPermissionDoc = {
    mutable Id : string
    mutable Writes: int
}

type CurrentDoc = (MyCountingDoc * RavenJObject * Etag)

type EventContext = {
    Tenancy : string
}

module RavenProjectorTests = 

    let buildDocumentStore () =
        let documentStore = new Raven.Client.Document.DocumentStore()
        documentStore.Url <- "http://localhost:8080"
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

        let beforeWrite = (fun (doc : MyCountingDoc, metadata, etag) ->
                doc.Writes <- doc.Writes + 1
                (doc, metadata, etag)
            )

        let processBatch key (fetcher : IDocumentFetcher) events = async {
            let docKey = "MyCountingDocs/" + key.ToString() 
            let permDocKey = "PermissionDocs/" + key.ToString() 
            let! (doc, metadata, etag) =  
                fetcher.GetDocument docKey
                |> Async.map (Option.getOrElseF (fun () -> buildNewDoc key))
               
            let! (permDoc, permMetadata, permEtag) =
                fetcher.GetDocument permDocKey
                |> Async.map (Option.getOrElseF (fun () -> ({ Id = permDocKey; Writes = 0 }, RavenOperations.emptyMetadata "PermissionDoc", Etag.Empty)))

            let (doc, metadata, etag) = 
                events
                |> Seq.fold (processEvent key) (doc, metadata, etag)

            let (doc, metadata, etag) = beforeWrite (doc, metadata, etag)

            permDoc.Writes <- permDoc.Writes + 1

            return seq {
                yield {
                    DocumentKey = docKey
                    Document = lazy(RavenJObject.FromObject(doc))
                    Metadata = lazy(metadata)
                    Etag = etag
                }
                yield {
                    DocumentKey = permDocKey
                    Document = lazy(RavenJObject.FromObject(permDoc))
                    Metadata = lazy(permMetadata)
                    Etag = permEtag
                }
            }
        }

        let myProcessor : DocumentProcessor<Guid, MyCountingDoc, EventContext> = {
            EventTypes = Seq.singleton typeof<int>
            MatchingKeys = matcher
            Process = processBatch
        }

        let processorSet = ProcessorSet.Empty.Add myProcessor
        let projector = new BulkRavenProjector<EventContext>(documentStore, processorSet, "tenancy-blue")

        seq {
            yield async {
                for event in myEvents do
                    do! projector.Enqueue event
                do! projector.WaitAll()
            }

            yield! seq {
                for key in streams do
                    yield (fun () -> async {
                        use session = documentStore.OpenAsyncSession("tenancy-blue")
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
            use session = documentStore.OpenAsyncSession("tenancy-blue")

            let! docs = session.Advanced.LoadStartingWithAsync<MyCountingDoc>("MyCountingDocs/", 0, 1024) |> Async.AwaitTask
            let! permDocs = session.Advanced.LoadStartingWithAsync<MyPermissionDoc>("PermissionDocs/",0, 1024) |> Async.AwaitTask
            let permDocs = 
                permDocs
                |> Seq.map (fun doc -> (doc.Id, doc.Writes))
                |> Map.ofSeq

            for doc in docs do
                doc.Count |> should equal 100
                doc.Foo |> should equal "Bar"
                doc.Value |> should equal -50
                doc.Writes |> should equal (permDocs.Item("PermissionDocs/" + doc.Id.ToString()))
            ()
        } |> Async.RunSynchronously
        ()