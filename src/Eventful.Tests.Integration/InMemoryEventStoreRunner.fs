namespace Eventful.Tests.Integration

open System
open System.IO
open System.Diagnostics
open Eventful
open EventStore.ClientAPI

module InMemoryEventStoreRunner =
    let logger = createLogger "Eventful.Tests.Integration.InMemoryEventStoreRunner"

    type EventStoreAccess =     
        { Process : Process
          Connection: IEventStoreConnection
          TcpPort : int
          HttpPort : int }
        interface IDisposable with
         member this.Dispose() =
             try
                 this.Connection.Dispose()
             with | ex ->
                 Console.WriteLine(sprintf "Exception disposing connection %A" ex)
                
             this.Process.Kill()
             this.Process.WaitForExit()
             this.Process.Dispose()


    let clusterNodeAbsolutePath = Path.Combine(IntegrationTests.buildDirectoryPath, "EventStore3\EventStore.ClusterNode.exe")
    let clusterNodeProcessName = "EventStore.ClusterNode"

    let startNewProcess () =
        let testTcpPort = IntegrationTests.findFreeTcpPort()
        let testHttpPort = IntegrationTests.findFreeTcpPort()
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

        let eventStoreProcess = IntegrationTests.startProcess clusterNodeAbsolutePath processArguments

        try
            let mutable started = false

            while not started do
                let line = eventStoreProcess.StandardOutput.ReadLine()
                if line <> null then
                    IntegrationTests.log.Debug (lazy line)
                if line <> null && line.Contains("SystemInit") then started <- true

            IntegrationTests.logOutput logger eventStoreProcess

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
            TcpPort = testTcpPort
            HttpPort = testHttpPort
            Connection = connection
        }