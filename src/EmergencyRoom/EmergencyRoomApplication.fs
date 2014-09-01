namespace EmergencyRoom

open System
open Newtonsoft.Json
open System.IO
open Eventful
open EventStore.ClientAPI
open Eventful.EventStore

type EmergencyRoomTopShelfService () =
    let log = createLogger "EmergencyRoom.EmergencyRoomTopShelfService"

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

    let mutable client : Client option = None
    let mutable eventStoreSystem : EventStoreSystem<unit,unit,EmergencyEventMetadata> option = None

    member x.Start () =
        log.Debug <| lazy "Starting App"
        async {
            let! connection = getConnection()
            let c = new Client(connection)

            let handlers =
                EventfulHandlers.empty
                |> EventfulHandlers.addAggregate Visit.handlers

            let system = new EventStoreSystem<unit,unit,EmergencyEventMetadata>(handlers, c, esSerializer, ())

            client <- Some c
            eventStoreSystem <- Some system
        } |> Async.StartAsTask
    member x.Stop () =
        log.Debug <| lazy "App Stopping"