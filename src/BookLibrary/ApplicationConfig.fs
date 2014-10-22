namespace BookLibrary

open System
open System.IO
open Newtonsoft.Json
open EventStore.ClientAPI
open Eventful
open Eventful.EventStore

type BookLibrarySystem (system : EventStoreSystem<unit,unit,BookLibraryEventMetadata>) = 
    interface IBookLibrarySystem with
        member x.RunCommand cmd =
            system.RunCommand () cmd

        member x.RunCommandTask cmd =
            system.RunCommand () cmd
            |> Async.StartAsTask

module ApplicationConfig = 
    let serializer = JsonSerializer.Create()

    let serialize (t : 'T) =
        use sw = new System.IO.StringWriter() :> System.IO.TextWriter
        serializer.Serialize(sw, t :> obj)
        System.Text.Encoding.UTF8.GetBytes(sw.ToString())

    let deserializeObj (v : byte[]) (typeName : string) : obj =
        let objType = Type.GetType typeName
        let str = System.Text.Encoding.UTF8.GetString(v)
        let reader = new StringReader(str) :> TextReader
        let result = serializer.Deserialize(reader, objType) 
        result

    let esSerializer = 
        { new ISerializer with
            member x.DeserializeObj b t = deserializeObj b t
            member x.Serialize o = serialize o }

    let getConnection () : Async<IEventStoreConnection> =
        async {
            let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("192.168.59.103"), 1113)
            let connectionSettingsBuilder = 
                ConnectionSettings
                    .Create()
                    .SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)
            connection.Connected.Add(fun _ ->  printf "Connected" )
            connection.ErrorOccurred.Add(fun e -> printfn "Error: %A" e.Exception )
            connection.Disconnected.Add(fun _ ->  printf "Disconnectiong" )

            return! connection.ConnectAsync().ContinueWith(fun t -> connection) |> Async.AwaitTask
        }

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate (Book.handlers ())
        |> EventfulHandlers.addAggregate (BookCopy.handlers ())

    let buildEventStoreSystem client =
        new EventStoreSystem<unit,unit,BookLibraryEventMetadata>(handlers, client, esSerializer, ())

    let initializedSystem () = 
        async {
            let! conn = getConnection ()
            let client = new Client(conn)
            let system = buildEventStoreSystem client
            return new BookLibrarySystem(system)
        } |> Async.StartAsTask

    let dbName = "BookLibrary"

    let buildDocumentStore() =
        let documentStore = new Raven.Client.Document.DocumentStore(Url = "http://localhost:8080/")
        documentStore.DefaultDatabase <- dbName
        documentStore.Initialize() |> ignore
        documentStore