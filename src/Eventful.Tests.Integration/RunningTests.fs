namespace Eventful.Tests.Integration

// TODO: REMOVE
//open Xunit
open EventStore.ClientAPI
open System
open System.IO
open System.Net
open Newtonsoft.Json
//open FsUnit.Xunit
open Eventful
open Eventful.EventStore
open EventStore.ClientAPI.Embedded
open EventStore.Core.Data;
//
module RunningTests = 
    let log = createLogger "Eventful.Tests.Integration.RunningTests"

    let getEmbeddedConnection () : Async<IEventStoreConnection> =
        // prints a warning but seems to work fine
        // let ipEndPoint = new System.Net.IPEndPoint(IPAddress.Loopback, 12342);

        let tcpEndpoint = new System.Net.IPEndPoint(IPAddress.Loopback, 12342)
        let httpEndpoint = new System.Net.IPEndPoint(IPAddress.Loopback, 12343)
        let embeddedInMemoryVNode = 
            EmbeddedVNodeBuilder
                .AsSingleNode()
                .WithInternalTcpOn(tcpEndpoint)
                .WithExternalTcpOn(tcpEndpoint)
                .WithExternalHttpOn(httpEndpoint)
                .WithInternalHttpOn(httpEndpoint)
                .RunInMemory()
                .RunProjections(ProjectionsMode.None)
                .WithWorkerThreads(16)

        let vNode = EmbeddedVNodeBuilder.op_Implicit(embeddedInMemoryVNode)

        let isMaster = new System.Threading.Tasks.TaskCompletionSource<bool>()
        vNode.NodeStatusChanged.Add(fun s -> if s.NewVNodeState = VNodeState.Master then isMaster.SetResult(true))
        vNode.Start()

        async {
            do! isMaster.Task |> Async.AwaitTask |> Async.Ignore
        
            let connection = EmbeddedEventStoreConnection.Create(vNode);
            connection.Connected.Add(fun _ -> log.Debug(lazy("Connected")))
            connection.ErrorOccurred.Add(fun e -> log.Debug(lazy(sprintf "Error: %A" e.Exception )))
            connection.Disconnected.Add(fun _ -> log.Debug(lazy("Disconnected")))

            connection.Closed.Add(fun _ -> vNode.Stop())

            return connection
        }

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

    let serializer = JsonSerializer.Create()

    let serialize<'T> (t : 'T) =
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