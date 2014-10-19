namespace BookLibrary

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

type TopShelfService () =
    let log = createLogger "EmergencyRoom.EmergencyRoomTopShelfService"

    let mutable client : Client option = None
    let mutable eventStoreSystem : EventStoreSystem<unit,unit,BookLibraryEventMetadata> option = None

    let matchingKeys (message : EventStoreMessage) = 
        Seq.empty

    let processDocuments documentStore (docKey:string, documentFetcher:IDocumentFetcher, messages : seq<EventStoreMessage>) =
        let f = (fun () -> 
                    async { return Seq.empty } |> Async.StartAsTask
                ) 
        new System.Func<Task<seq<ProcessAction>>>(f)

    member x.Start () =
        log.Debug <| lazy "Starting App"
        async {
            let! connection = ApplicationConfig.getConnection()
            let c = new Client(connection)

            let system = ApplicationConfig.buildEventStoreSystem c
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
                    let evtObj = ApplicationConfig.esSerializer.DeserializeObj re.Event.Data comparableType.RealType.FullName
                    let metadata = ApplicationConfig.esSerializer.DeserializeObj re.Event.Metadata typeof<BookLibraryEventMetadata>.FullName :?> BookLibraryEventMetadata

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