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
open Eventful.Tests
open Metrics

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

    type Dictionary<'Key,'Value> = System.Collections.Generic.Dictionary<'Key,'Value>

    [<Fact>]
    let ``Generate event stream`` () : unit =
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers 1000 100 |> Seq.cache
        consoleLog <| sprintf "Length %d" (myEvents |> Seq.length)
        ()

    [<Fact>]
    let ``Just complete tracking`` () : unit =
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers 1000 100 |> Seq.cache
        let tracker = new LastCompleteItemAgent2<EventPosition>()

        async {
            for event in myEvents do
                do! tracker.Start event.Context.Position

            for event in myEvents do
                tracker.Complete event.Context.Position

            let! result = tracker.LastComplete ()

            result |> should equal (Some { Commit = 99999L; Prepare = 99999L })
        } |> Async.RunSynchronously
        ()

    [<Fact>]
    let ``Pump many events at Raven`` () : unit =
        let config = Metric.Config.WithHttpEndpoint("http://localhost:8083/")
        
        let streamCount = 1000
        let itemPerStreamCount = 100
        let totalEvents = streamCount * itemPerStreamCount
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let streams = myEvents |> Seq.map (fun x -> Guid.Parse(x.StreamId)) |> Seq.distinct |> Seq.cache

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

        let matcher (subscriberEvent : SubscriberEvent) =
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

        let monitor = new System.Object()
        let itemsComplete = ref 0

        let writeComplete _ = async {
            lock(monitor) (fun () -> 
                itemsComplete := !itemsComplete + 1
            )
        }

        let processBatch key (fetcher : IDocumentFetcher) events = async {
            let docKey = "MyCountingDocs/" + key.ToString() 
            let permDocKey = "PermissionDocs/" + key.ToString() 
            let! (doc, metadata, etag) =  
                fetcher.GetDocument docKey
                |> Async.map (Option.getOrElseF (fun () -> buildNewDoc key))
               
            let! (permDoc, permMetadata, permEtag) =
                fetcher.GetDocument permDocKey
                |> Async.map (Option.getOrElseF (fun () -> ({ Id = permDocKey; Writes = 0 }, (RavenOperations.emptyMetadata<MyPermissionDoc> documentStore), Etag.Empty)))

            let (doc, metadata, etag) = 
                events
                |> Seq.fold (processEvent key) (doc, metadata, etag)

            let (doc, metadata, etag) = beforeWrite (doc, metadata, etag)

            permDoc.Writes <- permDoc.Writes + 1

            return seq {
                yield Write {
                    DocumentKey = docKey
                    Document = lazy(RavenJObject.FromObject(doc))
                    Metadata = lazy(metadata)
                    Etag = etag
                }
                yield Write {
                    DocumentKey = permDocKey
                    Document = lazy(RavenJObject.FromObject(permDoc))
                    Metadata = lazy(permMetadata)
                    Etag = permEtag
                }
            }
        }

        let myProcessor : DocumentProcessor<Guid, MyCountingDoc, SubscriberEvent> = {
            EventTypes = Seq.singleton typeof<(Guid * int)>
            MatchingKeys = matcher
            Process = processBatch
        }

        let processorSet = ProcessorSet.Empty.Add myProcessor

        let projector = new BulkRavenProjector<SubscriberEvent>(documentStore, processorSet, "tenancy-blue", 1000000, 10, 10000, 10, writeComplete)
        projector.StartWork()
        projector.StartPersistingPosition()

        seq {
            yield async {
                let sw = System.Diagnostics.Stopwatch.StartNew()
                for event in myEvents do
                    do! projector.Enqueue event

                projector.StartWork()
                do! projector.WaitAll()
                sw.Stop()

                consoleLog <| sprintf "Insert all time: %A ms" sw.ElapsedMilliseconds
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
            let! lastComplete = projector.LastComplete()
            consoleLog <| sprintf "Final Position: %A" lastComplete
            use session = documentStore.OpenAsyncSession("tenancy-blue")

            !itemsComplete |> should equal totalEvents

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