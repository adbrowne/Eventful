namespace Eventful.Tests.Integration

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Diagnostics
open EventStore.ClientAPI

module InMemoryEventStoreRunner =

    let findFreeTcpPort () =
        let l = new TcpListener(IPAddress.Loopback, 0)
        l.Start()
        let port = (l.LocalEndpoint :?> IPEndPoint).Port
        l.Stop()
        port
 
    type EventStoreAccess =     
        { Process : Process
          Connection: IEventStoreConnection }
        interface IDisposable with
         member this.Dispose() =
             Console.WriteLine("Disposing")
             try
                 this.Connection.Dispose()
             with | ex ->
                 Console.WriteLine(sprintf "Exception disposing connection %A" ex)
                
             this.Process.Kill()
             this.Process.WaitForExit()
             this.Process.Dispose()


    let clusterNodeAbsolutePath = Path.Combine(DirectoryInfo(Directory.GetCurrentDirectory()).Parent.FullName, "EventStore3\EventStore.ClusterNode.exe")
    let testNodeEnvironmentVariable = "EventfulTestNode"
    let clusterNodeProcessName = "EventStore.ClusterNode"

    let startNewProcess () =
        let testTcpPort = findFreeTcpPort()
        let testHttpPort = findFreeTcpPort()
        let processArguments = 
            let timeoutOptions = "--Int-Tcp-Heartbeat-Timeout=50000 --Ext-Tcp-Heartbeat-Timeout=50000"
            let portOptions = 
                sprintf 
                    "--int-tcp-port=%d --ext-tcp-port=%d --int-http-port=%d --ext-http-port=%d"
                    testTcpPort
                    testTcpPort
                    testHttpPort
                    testHttpPort
                
            sprintf 
                "--mem-db --run-projections=None %s %s"
                portOptions
                timeoutOptions

        let startInfo = 
            System.Diagnostics.ProcessStartInfo(
                clusterNodeAbsolutePath, 
                processArguments,
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true)
        startInfo.EnvironmentVariables.Add(testNodeEnvironmentVariable, "true")

        let eventStoreProcess = Process.Start(startInfo)

        try
            let mutable started = false

            while not started do
                let line = eventStoreProcess.StandardOutput.ReadLine()
                if line <> null then
                    IntegrationTests.log.Debug (lazy line)
                if line <> null && line.Contains("SystemInit") then started <- true
            (testTcpPort, testHttpPort, eventStoreProcess)
        with | _ ->
            eventStoreProcess.Kill()
            reraise()

    let connectToEventStore testTcpPort=
        let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, testTcpPort)
        let connectionSettingsBuilder = 
            ConnectionSettings
                .Create()
                .SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))

        let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

        let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)

        connection.ConnectAsync().Wait()

        IntegrationTests.runUntilSuccess 100 (fun () -> connection.ReadAllEventsForwardAsync(EventStore.ClientAPI.Position.Start, 1, false) |> Async.AwaitTask |> Async.RunSynchronously) |> ignore

        connection

    let startInMemoryEventStore () =
        let (testTcpPort, testHttpPort, eventStoreProcess) = startNewProcess ()
        let connection = connectToEventStore testTcpPort
        {
            Process = eventStoreProcess
            Connection = connection
        }