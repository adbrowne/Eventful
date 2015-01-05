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

type ConverterHelper () =
    static member ToFunc<'T> (x : Func<'T>) : Func<'T> = x

module RavenProjectorTests = 

    let log = createLogger "Eventful.Tests.RavenProjectorTests"

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
    let ``Insert 100,000 items into a sorted set`` () : unit =
        let rnd = new Random()

        let items = [1..100000] |> Seq.sortBy (fun _ -> rnd.Next(100))

        let sortedIntSet = new System.Collections.Generic.SortedSet<int>()

        let sw = System.Diagnostics.Stopwatch.StartNew()
        for item in items do
            sortedIntSet.Add(item) |> ignore
        sw.Stop()
        printfn "Time to add 100000 ints %d ms" sw.ElapsedMilliseconds

        let notificationItems = 
            items 
            |> Seq.map (fun item -> 
                {
                    Eventful.NotificationItem.Item = item
                    Tag = None
                    Callback = async { () }
                })

        let sw = System.Diagnostics.Stopwatch.StartNew()
        let sortedNotificationDictionary = new System.Collections.Generic.SortedDictionary<int,Eventful.NotificationItem<int>>()
        for item in notificationItems do
            if(not <| sortedNotificationDictionary.ContainsKey item.Item) then
                sortedNotificationDictionary.Add(item.Item, item) |> ignore
            else
                ()
        sw.Stop()
        printfn "Time to add 100000 notifications to sorted dictionary with existance check %d ms" sw.ElapsedMilliseconds

        let myAgent count agent = async {
            return ()
        }
            
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let sortedNotificationDictionary = new System.Collections.Generic.SortedDictionary<int,Eventful.NotificationItem<int>>()
        for item in notificationItems do
            let a = Agent.Start(myAgent 10)
            ()
        sw.Stop()
        printfn "Time to create 100000 agents %d ms" sw.ElapsedMilliseconds

    [<Fact>]
    let ``Generate event stream`` () : unit =
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers 1000 100 |> Seq.cache
        consoleLog <| sprintf "Length %d" (myEvents |> Seq.length)
        ()

    [<Fact>]
    let ``Just complete tracking`` () : unit =
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers 1000 100 |> Seq.cache
        let tracker = new LastCompleteItemAgent<EventPosition>()

        async {
            for event in myEvents do
                tracker.Start event.Context.Position

            for event in myEvents do
                tracker.Complete event.Context.Position

            let! result = tracker.LastComplete ()

            result |> should equal (Some { Commit = 99999L; Prepare = 99999L })
        } |> Async.RunSynchronously
        ()

    let ``Get Projector`` documentStore onComplete =
        let countingDocKeyPrefix =  "MyCountingDocs/"
        let countingDocKey (key : Guid) =
            countingDocKeyPrefix + key.ToString()  

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
                let (guid, _) = event
                guid |> Seq.singleton
            | _ -> Seq.empty

        let processEvent doc subscriberEvent = 
            match subscriberEvent.Event with
            | :? (Guid * int) as event ->
                let (key, value) = event
                processValue value doc
            | _ -> doc

        let processBatch (fetcher : IDocumentFetcher) (docId : Guid) events = async {
            let requestId = Guid.NewGuid()
            let docKey = countingDocKey docId
            let permDocKey = "PermissionDocs/" + (docKey.ToString())
            let! (doc, metadata, etag) =  
                fetcher.GetDocument docKey
                |> Async.AwaitTask
                |> Async.map (Option.getOrElseF (fun () -> buildNewDoc docId))
               
            let! (permDoc, permMetadata, permEtag) =
                fetcher.GetDocument permDocKey
                |> Async.AwaitTask
                |> Async.map (Option.getOrElseF (fun () -> ({ Id = permDocKey; Writes = 0 }, (RavenOperations.emptyMetadata<MyPermissionDoc> documentStore), Etag.Empty)))

            let (doc, metadata, etag) = 
                events
                |> Seq.fold (processEvent) (doc, metadata, etag)

            let beforeWrite = (fun (doc : MyCountingDoc, metadata, etag) ->
                doc.Writes <- doc.Writes + 1
                (doc, metadata, etag)
            )

            let (doc, metadata, etag) = beforeWrite (doc, metadata, etag)

            permDoc.Writes <- permDoc.Writes + 1

            return (seq {
                yield (
                        Write ({
                                DocumentKey = docKey
                                Document = doc
                                Metadata = lazy(metadata)
                                Etag = etag
                            }, 
                            requestId))
                yield (
                        Write({
                                DocumentKey = permDocKey
                                Document = permDoc
                                Metadata = lazy(permMetadata)
                                Etag = permEtag
                            },
                            requestId))
            }, onComplete)
        }

        let projector = {
            Projector.MatchingKeys = matcher
            ProcessEvents = processBatch
        }

        projector 
        :> IProjector<_,_,_>
        |> Seq.singleton

    let testDatabase = "tenancy-blue"

    let buildRavenProjector documentStore documentProjectors onWriteComplete = 
        let cache = new System.Runtime.Caching.MemoryCache("RavenBatchWrite")

        let cancellationToken = Async.DefaultCancellationToken

        let writeQueue = new RavenWriteQueue(documentStore, 100, 10000, 10, cancellationToken, cache)
        let readQueue = new RavenReadQueue(documentStore, 100, 10000, 10, cancellationToken,  cache)

        let projector =
            BulkRavenProjector.create(
                testDatabase,
                documentProjectors,
                cancellationToken,
                onWriteComplete,
                documentStore,
                writeQueue,
                readQueue,
                1000000,
                1000,
                None,
                1000)

        projector

    let ``Get Raven Projector With onComplete`` documentStore onComplete = 
        let monitor = new System.Object()
        let itemsComplete = ref 0

        let writeComplete _ = async {
            lock(monitor) (fun () -> 
                itemsComplete := !itemsComplete + 1
            )
        }
        
        let myProjector = ``Get Projector`` documentStore onComplete

        buildRavenProjector documentStore myProjector writeComplete
    
    let ``Get Raven Projector`` documentStore = 
        ``Get Raven Projector With onComplete`` documentStore (async.Zero())
  
    [<Fact>]
    let ``Enqueue events into projector`` () : unit =   
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

        let streamCount = 1000
        let itemPerStreamCount = 100
        let totalEvents = streamCount * itemPerStreamCount
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let streams = myEvents |> Seq.map (fun x -> Guid.Parse(x.StreamId)) |> Seq.distinct |> Seq.cache

        let projector = ``Get Raven Projector`` documentStore

        async {
            let sw = System.Diagnostics.Stopwatch.StartNew()
            for event in myEvents do
                do! projector.Enqueue event

            sw.Stop()
            consoleLog <| sprintf "Enqueue Time: %A ms" sw.ElapsedMilliseconds

            // do! projector.WaitAll()
        } |> Async.RunSynchronously


    [<Fact>]
    let ``Run Events Through Grouping Queue`` () : unit =   

        let streamCount = 10000
        let itemPerStreamCount = 100
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let queue = new MutableOrderedGroupingBoundedQueue<string, SubscriberEvent>(100000000, "My Queue")

        let sw = System.Diagnostics.Stopwatch.StartNew()
        let working = ref false

        let lastDone = ref DateTime.Now.Ticks
        for i in [1..100] do
            async {
                while(true) do  
                    if not !working then
                        do! Async.Sleep 2000
                    else
                        let! work = queue.Consume (fun _ -> async { () })
                        do! work
                        lastDone := DateTime.Now.Ticks
                return ()
            } |> Async.StartAsTask |> ignore

        async {
            for event in myEvents do
                do! queue.Add(event, fun _ -> Seq.singleton (event, event.StreamId))
        }
        |> Async.RunSynchronously

        consoleLog <| sprintf "Enqueue Time: %A ms" sw.ElapsedMilliseconds
        sw.Restart()

        sw.Restart()
        working := true
        queue.CurrentItemsComplete () |> Async.RunSynchronously
        sw.Stop()
        let completeTime = sw.ElapsedMilliseconds
        let ticksSinceLastComplete = DateTime.Now.Ticks - !lastDone
        consoleLog <| sprintf "Complete %A ms" sw.ElapsedMilliseconds
        consoleLog <| sprintf "Just waiting for notify to catch up %A ms" (new TimeSpan(ticksSinceLastComplete)).TotalMilliseconds
        consoleLog <| sprintf "Complete Rate %A /s" (double streamCount / sw.Elapsed.TotalSeconds)


    [<Fact>]
    let ``Deal with timeouts gracefully`` () : unit =
        use config = Metric.Config.WithHttpEndpoint("http://localhost:8083/")
        
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

        let streamCount = 1
        let itemPerStreamCount = 1
        let totalEvents = streamCount * itemPerStreamCount
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let streams = myEvents |> Seq.map (fun x -> Guid.Parse(x.StreamId)) |> Seq.distinct |> Seq.cache

        let projector = ``Get Raven Projector`` documentStore
        // projector.StartWork()
        // projector.StartPersistingPosition()

        seq {
            yield async {
                let sw = System.Diagnostics.Stopwatch.StartNew()
                for event in myEvents do
                    do! projector.Enqueue event

                consoleLog <| sprintf "Enqueue Time: %A ms" sw.ElapsedMilliseconds

                projector.StartWork()
                do! projector.WaitAll()
                sw.Stop()

                consoleLog <| sprintf "Insert all time: %A ms" sw.ElapsedMilliseconds
            }
        }
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

    [<Fact>]
    let ``Test Speed of LastCompleteItemAgent`` () : unit =   
        let random = new Random(4)

        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        let lastItem = 1000000L
        let start = [0L..lastItem]
        let completeOrder = 
            start 
            |> List.sortBy (fun _ -> random.Next())

        let sw = System.Diagnostics.Stopwatch.StartNew()
        let myLastCompleteItemAgent = new LastCompleteItemAgent<int64>()
        for i in start do
            myLastCompleteItemAgent.Start i |> ignore

        myLastCompleteItemAgent.NotifyWhenComplete (lastItem, Some "Ignore", async { tcs.SetResult(true) })

        let startTime = sw.ElapsedMilliseconds
        sw.Restart()

        for i in completeOrder do
            myLastCompleteItemAgent.Complete i

        let completeTime = sw.ElapsedMilliseconds
        sw.Restart()

        tcs.Task.Wait()

        let notifyTime = sw.ElapsedMilliseconds

        consoleLog <| sprintf "Start time: %A" startTime
        consoleLog <| sprintf "completeTime: %A" completeTime
        consoleLog <| sprintf "notifyTime: %A" notifyTime

    [<Fact>]
    let ``Pump many events at Raven with concurrency`` () : unit =
        use config = Metric.Config.WithHttpEndpoint("http://localhost:8083/")
        
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

        let streamCount = 1000
        let itemPerStreamCount = 100
        let totalEvents = streamCount * itemPerStreamCount
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let streams = myEvents |> Seq.map (fun x -> Guid.Parse(x.StreamId)) |> Seq.distinct |> Seq.cache

        let projector = ``Get Raven Projector`` documentStore
        //projector.StartWork()
        projector.StartPersistingPosition()

        seq {
            yield async {
                let sw = System.Diagnostics.Stopwatch.StartNew()
                for event in myEvents do
                    do! projector.Enqueue event

                consoleLog <| sprintf "Enqueue Time: %A ms" sw.ElapsedMilliseconds

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

            // !itemsComplete |> should equal totalEvents

            let! docs = session.Advanced.LoadStartingWithAsync<MyCountingDoc>("MyCountingDocs/", 0, 1024) |> Async.AwaitTask
            let! permDocs = session.Advanced.LoadStartingWithAsync<MyPermissionDoc>("PermissionDocs/",0, 1024) |> Async.AwaitTask
            let permDocs = 
                permDocs
                |> Seq.map (fun doc -> (doc.Id, doc.Writes))
                |> Map.ofSeq

            for (doc : MyCountingDoc) in docs do
                doc.Count |> should equal 100
                doc.Foo |> should equal "Bar"
                doc.Value |> should equal -50
                doc.Writes |> should equal (permDocs.Item("PermissionDocs/MyCountingDocs/" + doc.Id.ToString()))
                ()
            ()
        } |> Async.RunSynchronously

        ()

    [<Fact>]
    let ``Pump many events at Raven`` () : unit =
        use config = Metric.Config.WithHttpEndpoint("http://localhost:8083/")
        
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

        let streamCount = 10000
        let itemPerStreamCount = 100
        let totalEvents = streamCount * itemPerStreamCount
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let streams = myEvents |> Seq.map (fun x -> Guid.Parse(x.StreamId)) |> Seq.distinct |> Seq.cache

        let projector = ``Get Raven Projector`` documentStore
        //projector.StartWork()
        projector.StartPersistingPosition()

        seq {
            yield async {
                let sw = System.Diagnostics.Stopwatch.StartNew()
                for event in myEvents do
                    do! projector.Enqueue event

                let enqueueTicks = sw.ElapsedTicks
                consoleLog <| sprintf "Enqueue Time: %A ms" sw.ElapsedMilliseconds

                projector.StartWork()
                do! projector.WaitAll()
                sw.Stop()

                consoleLog <| sprintf "Insert all time: %A ms" sw.ElapsedMilliseconds
                let insertOnlyTime = new TimeSpan(sw.ElapsedTicks - enqueueTicks)
                consoleLog <| sprintf "Insert rate %A/s" (double streamCount / double insertOnlyTime.TotalSeconds)
                let docCount = streamCount * 2 // permission docs double the number
                consoleLog <| sprintf "Insert rate %A/s" (double docCount / double insertOnlyTime.TotalSeconds)
            }
        }
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        ()

    [<Fact>]
    let ``OnComplete is triggered after batch completed`` () : unit =
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 

        let streamCount = 10
        let itemPerStreamCount = 100
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let completeCount = ref 0
        let onComplete = async {
            System.Threading.Interlocked.Increment completeCount |> ignore
        }

        let projector = ``Get Raven Projector With onComplete`` documentStore onComplete

        async {
            for event in myEvents do
                do! projector.Enqueue event

            Assert.Equal(0, !completeCount)

            projector.StartWork()
            do! projector.WaitAll()

            Assert.Equal(streamCount, !completeCount)
        } |> Async.RunSynchronously
