namespace BookLibrary

open System
open Eventful
open Eventful.EventStore
open EventStore.ClientAPI
open Eventful.Raven
open Suave
open Suave.Http
open Suave.Web

module SetupHelpers =
    let buildDocumentStore (ravenConfig : RavenConfig) =
        let documentStore = new Raven.Client.Document.DocumentStore(Url = sprintf "http://%s:%d/" ravenConfig.Server ravenConfig.Port)
        documentStore.Initialize() |> ignore
        documentStore :> Raven.Client.IDocumentStore

    let addEventTypes evtTypes handlers =
        Array.fold (fun h x -> StandardConventions.addEventType x h) handlers evtTypes

    let eventTypes =
        System.Reflection.Assembly.GetExecutingAssembly()
        |> Eventful.Utils.getLoadableTypes

    let toString x = x.ToString()

    let handlers openSession : EventfulHandlers<_,_,_,IEvent> =
        EventfulHandlers.empty (BookLibraryEventMetadata.GetAggregateType >> toString)
        |> EventfulHandlers.addAggregate (Book.handlers openSession)
        |> EventfulHandlers.addAggregate (BookCopy.handlers openSession)
        |> EventfulHandlers.addAggregate (Award.handlers ())
        |> EventfulHandlers.addAggregate (Delivery.handlers ())
        |> EventfulHandlers.addAggregate NewArrivalsNotification.handlers
        |> addEventTypes eventTypes

type BookLibraryServiceRunner (applicationConfig : ApplicationConfig) =
    let log = createLogger "BookLibrary.BookLibraryServiceRunner"
    let webConfig = applicationConfig.WebServer
    let ravenConfig = applicationConfig.Raven
    let eventStoreConfig = applicationConfig.EventStore

    let mutable client : EventStoreClient option = None
    let mutable eventStoreSystem : BookLibraryEventStoreSystem option = None

    let getIpAddress server = 
        let addresses = 
            System.Net.Dns.GetHostAddresses server
            |> Array.filter (fun x -> x.AddressFamily = Net.Sockets.AddressFamily.InterNetwork)
            |> List.ofArray

        match addresses with
        | [] -> failwith <| sprintf "Could not find IPv4 address for %s" server
        | x::xs -> x
        
    let getIpEndpoint server port =
        new System.Net.IPEndPoint(getIpAddress server, port)
        
    let getConnection (eventStoreConfig : EventStoreConfig) : Async<IEventStoreConnection> =
        async {
            let ipEndPoint = getIpEndpoint eventStoreConfig.Server eventStoreConfig.TcpPort
            let connectionSettingsBuilder = 
                ConnectionSettings
                    .Create()
                    .SetDefaultUserCredentials(new SystemData.UserCredentials(eventStoreConfig.Username, eventStoreConfig.Password))
                    .KeepReconnecting()
                    .SetHeartbeatTimeout(TimeSpan.FromMinutes 5.0)
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)
            connection.Connected.Add(fun _ -> log.RichDebug "EventStore Connected" [||])
            connection.ErrorOccurred.Add(fun e -> log.ErrorWithException <| lazy("EventStore Connection Error", e.Exception))
            connection.Disconnected.Add(fun _ -> log.RichDebug "EventStore Disconnected" [||])

            log.RichDebug "EventStore Connecting" [||]
            return! connection.ConnectAsync().ContinueWith(fun t -> connection) |> Async.AwaitTask
        }

    let buildWakeupMonitor documentStore onWakeups = 
        new Eventful.Raven.WakeupMonitor(documentStore, ravenConfig.Database, onWakeups) :> Eventful.IWakeupMonitor

    let buildEventStoreSystem (documentStore : Raven.Client.IDocumentStore) client =
        let getSnapshot = Eventful.Raven.AggregateStatePersistence.getStateSnapshot documentStore Serialization.esSerializer ravenConfig.Database
        let openSession () = documentStore.OpenAsyncSession(ravenConfig.Database)
        let wakeupMonitor = buildWakeupMonitor documentStore
        new BookLibraryEventStoreSystem(SetupHelpers.handlers openSession, client, Serialization.esSerializer, (fun pe -> { BookLibraryEventContext.Metadata = pe.Metadata; EventId = pe.EventId }), getSnapshot, wakeupMonitor)

    let initializedSystem documentStore eventStoreConfig = 
        async {
            let! conn = getConnection eventStoreConfig
            let client = new EventStoreClient(conn)
            let system = buildEventStoreSystem documentStore client
            return new BookLibrarySystem(system)
        } |> Async.StartAsTask

    let runAsyncAsTask f =
        async {
            try 
                do! f
            with | e -> 
                log.ErrorWithException <| lazy("Exception starting EventStoreSystem",e)
                raise ( new System.Exception("See inner exception",e)) // cannot use reraise in an async block
        } |> Async.StartAsTask

    member x.Start () =
        log.Debug <| lazy "Starting App"
        async {
            let! connection = getConnection eventStoreConfig
            let c = new EventStoreClient(connection)

            let documentStore = SetupHelpers.buildDocumentStore ravenConfig

            let system : BookLibraryEventStoreSystem = buildEventStoreSystem documentStore c
            system.Start() |> Async.StartAsTask |> ignore

            let bookLibrarySystem = new BookLibrarySystem(system)

            let dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(ravenConfig.Database)

            let webAddress = getIpAddress webConfig.Server

            let suaveLogger = new SuaveEventfulLogger(Serilog.Log.Logger.ForContext("SourceContext","Suave"))
            let suaveConfig = 
                { default_config with 
                   Types.SuaveConfig.bindings = [Types.HttpBinding.Create (Types.Protocol.HTTP, webAddress.ToString(), webConfig.Port)] 
                   logger = suaveLogger }

            // start web
            let (ready, listens) =
                choose 
                    [ BooksWebApi.config bookLibrarySystem
                      BooksCopiesWebApi.config bookLibrarySystem
                      AwardsWebApi.config bookLibrarySystem
                      DeliveryWebApi.config bookLibrarySystem
                      FileWebApi.config dbCommands
                      (Suave.Http.RequestErrors.NOT_FOUND "404 Not Found") ]
                |> web_server_async suaveConfig
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
                        ravenConfig.Database,
                        [projector; aggregateStateProjector],
                        Async.DefaultCancellationToken,  
                        (fun _ -> async { () }),
                        documentStore,
                        writeQueue,
                        readQueue,
                        100000, 
                        1000, 
                        Some (TimeSpan.FromSeconds(60.0)),
                        5000
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
        } |> runAsyncAsTask

    member x.Stop () =
        log.Debug <| lazy "App Stopping"