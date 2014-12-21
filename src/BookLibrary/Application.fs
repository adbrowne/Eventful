namespace BookLibrary

open System
open Eventful
open Eventful.EventStore
open Eventful.Raven
open Suave
open Suave.Http
open Suave.Web

type BookLibraryServiceRunner () =
    let log = createLogger "BookLibrary"

    let mutable client : Client option = None
    let mutable eventStoreSystem : BookLibraryEventStoreSystem option = None

    member x.Start () =

        log.Debug <| lazy "Starting App"
        async {
            let! connection = ApplicationConfig.getConnection()
            let c = new Client(connection)

            let documentStore = ApplicationConfig.buildDocumentStore()

            let system : BookLibraryEventStoreSystem = ApplicationConfig.buildEventStoreSystem documentStore c
            system.Start() |> Async.StartAsTask |> ignore

            let bookLibrarySystem = new BookLibrarySystem(system)

            let dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(ApplicationConfig.dbName)

            // start web
            let (ready, listens) =
                choose 
                    [ BooksWebApi.config bookLibrarySystem
                      BooksCopiesWebApi.config bookLibrarySystem
                      AwardsWebApi.config bookLibrarySystem
                      DeliveryWebApi.config bookLibrarySystem
                      FileWebApi.config dbCommands
                      (Suave.Http.RequestErrors.NOT_FOUND "404 Not Found") ]
                |> web_server_async default_config 
            listens |> Async.Start

            let projector = 
                DocumentBuilderProjector.buildProjector 
                    documentStore 
                    Book.documentBuilder
                    (fun (m:EventStoreMessage) -> m.Event)
                    (fun (m:EventStoreMessage) -> m.EventContext)
                :> IProjector<_,_,_>

            let aggregateStateProjector = 
                AggregateStatePersistence.buildProjector
                    (EventStoreMessage.ToPersitedEvent >> Some)
                    Serialization.esSerializer
                    system.Handlers

            let cache = new System.Runtime.Caching.MemoryCache("myCache")

            let writeQueue = new RavenWriteQueue(documentStore, 100, 10000, 10, Async.DefaultCancellationToken, cache)
            let readQueue = new RavenReadQueue(documentStore, 100, 1000, 10, Async.DefaultCancellationToken, cache)

            let bulkRavenProjector =    
                BulkRavenProjector.create
                    (
                        ApplicationConfig.dbName,
                        [projector; aggregateStateProjector],
                        Async.DefaultCancellationToken,  
                        (fun _ -> async { () }),
                        documentStore,
                        writeQueue,
                        readQueue,
                        100000, 
                        1000, 
                        Some (TimeSpan.FromSeconds(60.0))
                    )
            bulkRavenProjector.StartWork ()
            bulkRavenProjector.StartPersistingPosition ()

            let lastPosition = 
                bulkRavenProjector.LastComplete() 
                |> Async.RunSynchronously 
                |> Option.map (fun eventPosition -> new EventStore.ClientAPI.Position(eventPosition.Commit, eventPosition.Prepare))

            let handle id (re : EventStore.ClientAPI.ResolvedEvent) =
                log.Debug <| lazy(sprintf "Projector received event : %s" re.Event.EventType)
                match system.EventStoreTypeToClassMap.ContainsKey re.Event.EventType with
                | true ->
                    let eventClass = system.EventStoreTypeToClassMap.Item re.Event.EventType
                    let evtObj = Serialization.esSerializer.DeserializeObj re.Event.Data eventClass
                    let metadata = Serialization.esSerializer.DeserializeObj re.Event.Metadata typeof<BookLibraryEventMetadata> :?> BookLibraryEventMetadata

                    let eventStoreMessage : EventStoreMessage = {
                        EventContext = metadata
                        Id = re.Event.EventId
                        Event = evtObj
                        StreamIndex = re.Event.EventNumber
                        EventPosition = { Commit = re.OriginalPosition.Value.CommitPosition; Prepare = re.OriginalPosition.Value.PreparePosition }
                        StreamName = re.Event.EventStreamId
                        EventType = re.Event.EventType
                    }

                    bulkRavenProjector.Enqueue (eventStoreMessage)
                | false -> async { () }

            let onLive _ = ()

            log.Debug <| lazy(sprintf "About to subscribe projector")
            c.subscribe lastPosition handle onLive |> ignore
            log.Debug <| lazy(sprintf "Subscribed projector")

            client <- Some c
            eventStoreSystem <- Some system
        } |> Async.StartAsTask

    member x.Stop () =
        log.Debug <| lazy "App Stopping"