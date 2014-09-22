namespace EmergencyRoom

open System
open System.IO
open Eventful
open EventStore.ClientAPI
open Eventful.EventStore
open Eventful.Raven
open System.Threading.Tasks

type EventCountDoc = {
    Count : int
}

type EmergencyRoomTopShelfService () =
    let log = createLogger "EmergencyRoom.EmergencyRoomTopShelfService"

    let mutable client : Client option = None
    let mutable eventStoreSystem : EventStoreSystem<unit,unit,EmergencyEventMetadata> option = None

    let matchingKeys (message : EventStoreMessage) = seq {
        yield "EventCount/" + message.Event.GetType().ToString()

        let isVisitEvent = 
            Visit.visitDocumentBuilder.Types
            |> List.exists (fun x -> 
                                let areEqual = x = message.Event.GetType()
                                printfn "areEqual %s %s %b" (x.Name) (message.Event.GetType().Name) areEqual
                                areEqual
                                )

        if isVisitEvent then    
            printfn "is a visit event"
            let visitId : VisitId = MagicMapper.magicId message.Event
            yield "Visit/" + visitId.Id.ToString()
    }

    let processEventCount documentStore (docKey:string, documentFetcher:IDocumentFetcher, messages : seq<'TMessage>) = async {
                        let! doc = documentFetcher.GetDocument<EventCountDoc>(docKey) |> Async.AwaitTask
                        let (doc, metadata, etag) = 
                            match doc with
                            | Some x -> x
                            | None -> 
                                let doc = { Count = 0 }
                                let metadata = RavenOperations.emptyMetadata<EventCountDoc> documentStore
                                let etag = Raven.Abstractions.Data.Etag.Empty
                                (doc, metadata, etag)

                        let doc = { doc with Count = doc.Count + 1 }
                        return seq {
                            let write = {
                               DocumentKey = docKey
                               Document = doc
                               Metadata = lazy(metadata) 
                               Etag = etag
                            }
                            yield Write (write, Guid.NewGuid())
                        }
    }

    let processVisitEvent documentStore (docKey:string, documentFetcher:IDocumentFetcher, messages : seq<EventStoreMessage>) = async {
                        let! doc = documentFetcher.GetDocument<Visit.VisitDocument>(docKey) |> Async.AwaitTask
                        let visitId : VisitId = 
                            messages
                            |> Seq.head
                            |> (fun x -> MagicMapper.magicId x.Event)
                        let (doc, metadata, etag) = 
                            match doc with
                            | Some x -> x
                            | None -> 
                                let doc = Visit.VisitDocument.NewDoc visitId
                                let metadata = RavenOperations.emptyMetadata<Visit.VisitDocument> documentStore
                                let etag = Raven.Abstractions.Data.Etag.Empty
                                (doc, metadata, etag)

                        let doc = 
                            messages
                            |> Seq.map (fun x -> x.Event)
                            |> Seq.fold Visit.visitDocumentBuilder.Run (Some doc)
                            |> function
                            | Some doc -> doc
                            | None -> Visit.VisitDocument.NewDoc visitId

                        return seq {
                            let write = {
                               DocumentKey = docKey
                               Document = doc
                               Metadata = lazy(metadata) 
                               Etag = etag
                            }
                            yield Write (write, Guid.NewGuid())
                        }
    }

    let processDocuments documentStore (docKey:string, documentFetcher:IDocumentFetcher, messages : seq<EventStoreMessage>) =
        let f = (fun () -> 
                    if docKey.StartsWith("EventCount/") then
                        processEventCount documentStore (docKey, documentFetcher, messages) |> Async.StartAsTask
                    else
                        processVisitEvent documentStore (docKey, documentFetcher, messages) |> Async.StartAsTask
                ) 
        new System.Func<Task<seq<ProcessAction>>>(f)

    member x.Start () =
        log.Debug <| lazy "Starting App"
        async {
            let! connection = EmergencyRoomApplicationConfig.getConnection()
            let c = new Client(connection)

            let system = EmergencyRoomApplicationConfig.buildEventStoreSystem c
            system.Start() |> Async.StartAsTask |> ignore

            let documentStore = new Raven.Client.Document.DocumentStore(Url = "http://localhost:8080")
            documentStore.Initialize() |> ignore
            let dbName = "EmergencyRoom"

            let documentProcessor = {
                MatchingKeys = matchingKeys
                Process = processDocuments documentStore
            } 

            let cache = new System.Runtime.Caching.MemoryCache("myCache")

            let writeQueue = new RavenWriteQueue(documentStore, 100, 10000, 10, Async.DefaultCancellationToken, cache)
            let readQueue = new RavenReadQueue(documentStore, 100, 1000, 10, Async.DefaultCancellationToken, cache)

            let bulkRavenProjector =    
                new BulkRavenProjector<EventStoreMessage>
                    (
                        documentStore,
                        documentProcessor,
                        dbName, 
                        100000, 
                        1000, 
                        (fun _ -> async { () }), Async.DefaultCancellationToken,  
                        writeQueue,
                        readQueue,
                        Some (TimeSpan.FromSeconds(60.0))
                    )
            bulkRavenProjector.StartWork ()
            bulkRavenProjector.StartPersistingPosition ()

            let lastPosition = 
                bulkRavenProjector.LastComplete() 
                |> Async.RunSynchronously 
                |> Option.map (fun eventPosition -> new EventStore.ClientAPI.Position(eventPosition.Commit, eventPosition.Prepare))

            let handle id (re : EventStore.ClientAPI.ResolvedEvent) =
                match system.EventTypeMap.TryFind re.Event.EventType with
                | Some comparableType ->
                    let evtObj = EmergencyRoomApplicationConfig.esSerializer.DeserializeObj re.Event.Data comparableType.RealType.FullName
                    let metadata = EmergencyRoomApplicationConfig.esSerializer.DeserializeObj re.Event.Metadata typeof<EmergencyEventMetadata>.FullName :?> EmergencyEventMetadata

                    let eventStoreMessage : EventStoreMessage = {
                        EventContext = metadata
                        Id = re.Event.EventId
                        Event = evtObj
                        StreamIndex = re.Event.EventNumber
                        EventPosition = { Commit = re.OriginalPosition.Value.CommitPosition; Prepare = re.OriginalPosition.Value.PreparePosition }
                        StreamName = re.Event.EventStreamId
                    }

                    bulkRavenProjector.Enqueue (eventStoreMessage)
                | None -> async { () }

            let onLive _ = ()

            c.subscribe lastPosition handle onLive |> ignore

            client <- Some c
            eventStoreSystem <- Some system
        } |> Async.StartAsTask

    member x.Stop () =
        log.Debug <| lazy "App Stopping"