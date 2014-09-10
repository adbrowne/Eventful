namespace EmergencyRoom

open System
open System.IO
open Newtonsoft.Json
open EventStore.ClientAPI
open Eventful
open Eventful.EventStore

type EmergencyRoomSystem (system : EventStoreSystem<unit,unit,EmergencyEventMetadata>) = 
    member x.RunCommand cmd =
        system.RunCommand () cmd
        |> Async.StartAsTask

module EmergencyRoomApplicationConfig = 
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
            let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
            let tcs = new System.Threading.Tasks.TaskCompletionSource<unit>()
            let connectionSettingsBuilder = 
                ConnectionSettings
                    .Create()
                    .OnConnected(fun _ _ -> printf "Connected"; )
                    .OnErrorOccurred(fun _ ex -> printfn "Error: %A" ex)
                    .SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)

            return! connection.ConnectAsync().ContinueWith(fun t -> connection) |> Async.AwaitTask
        }

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate Visit.handlers

    let buildEventStoreSystem client =
        new EventStoreSystem<unit,unit,EmergencyEventMetadata>(handlers, client, esSerializer, ())

    let initializedSystem () = 
        async {
            let! conn = getConnection ()
            let client = new Client(conn)
            let system = buildEventStoreSystem client
            return new EmergencyRoomSystem(system)
        } |> Async.StartAsTask