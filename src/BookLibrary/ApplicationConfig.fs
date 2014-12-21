namespace BookLibrary

open System
open EventStore.ClientAPI
open Eventful
open Eventful.EventStore
open FSharpx

type BookLibrarySystem (system : BookLibraryEventStoreSystem) = 
    interface IBookLibrarySystem with
        member x.RunCommand cmd =
            system.RunCommand () cmd

        member x.RunCommandTask cmd =
            system.RunCommand () cmd
            |> Async.StartAsTask

module ApplicationConfig = 
    let getConnection () : Async<IEventStoreConnection> =
        async {
            let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
            let connectionSettingsBuilder = 
                ConnectionSettings
                    .Create()
                    .SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
                    .KeepReconnecting()
                    .SetHeartbeatTimeout(TimeSpan.FromMinutes 5.0)
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)
            connection.Connected.Add(fun _ ->  printf "Connected" )
            connection.ErrorOccurred.Add(fun e -> printfn "Error: %A" e.Exception )
            connection.Disconnected.Add(fun _ ->  printf "Disconnectiong" )

            return! connection.ConnectAsync().ContinueWith(fun t -> connection) |> Async.AwaitTask
        }

    let addEventTypes evtTypes handlers =
        Array.fold (fun h x -> StandardConventions.addEventType x h) handlers evtTypes

    let eventTypes =
        System.Reflection.Assembly.GetExecutingAssembly()
        |> Eventful.Utils.getLoadableTypes

    let handlers openSession : EventfulHandlers<_,_,_,IEvent,_> =
        EventfulHandlers.empty BookLibraryEventMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (Book.handlers openSession)
        |> EventfulHandlers.addAggregate (BookCopy.handlers openSession)
        |> EventfulHandlers.addAggregate (Award.handlers ())
        |> EventfulHandlers.addAggregate (Delivery.handlers ())
        |> addEventTypes eventTypes

    let nullGetSnapshot streamId typeMap = StateSnapshot.Empty |> Async.returnM

    let dbName = "BookLibrary"

    let buildWakeupMonitor documentStore onWakeups = 
        new Eventful.Raven.WakeupMonitor<AggregateType>(documentStore, dbName, Serialization.esSerializer, onWakeups) :> Eventful.IWakeupMonitor

    let buildEventStoreSystem (documentStore : Raven.Client.IDocumentStore) client =
        let openSession () = documentStore.OpenAsyncSession(dbName)
        new BookLibraryEventStoreSystem(handlers openSession, client, Serialization.esSerializer, (fun pe -> { BookLibraryEventContext.Metadata = pe.Metadata; EventId = pe.EventId }), nullGetSnapshot, buildWakeupMonitor documentStore)

    let initializedSystem documentStore = 
        async {
            let! conn = getConnection ()
            let client = new Client(conn)
            let system = buildEventStoreSystem documentStore client
            return new BookLibrarySystem(system)
        } |> Async.StartAsTask

    let buildDocumentStore() =
        let documentStore = new Raven.Client.Document.DocumentStore(Url = "http://localhost:8080/")
        documentStore.DefaultDatabase <- dbName
        documentStore.Initialize() |> ignore
        documentStore