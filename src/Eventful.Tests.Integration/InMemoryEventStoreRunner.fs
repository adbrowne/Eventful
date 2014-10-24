namespace Eventful.Tests.Integration

open System
open System.IO
open System.Diagnostics
open EventStore.ClientAPI

module InMemoryEventStoreRunner =

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


    let eventStoreDirectory = "..\EventStore3"
    let installCompleteMarkerFile = Path.Combine(eventStoreDirectory, "test_setup.mrk")
    let clusterNodeExecutable = Path.Combine(eventStoreDirectory, "EventStore.ClusterNode.exe")
    let testClusterNodeProcessName = "EventStore.ClusterNode.Test"
    let testClusterNodeExecutable = Path.Combine(eventStoreDirectory, testClusterNodeProcessName + ".exe")
    let testTcpPort = 11130
    let testHttpPort = 21130

    let ensureNoZombieEventStores () =
        for proc in Process.GetProcessesByName(testClusterNodeProcessName) do
            IntegrationTests.runUntilSuccess 100 (fun () -> proc.Kill(); proc.WaitForExit())

    let startNewProcess () =
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
                testClusterNodeExecutable, 
                processArguments,
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true)

        let eventStoreProcess = Process.Start(startInfo)

        try
            let mutable started = false

            while not started do
                let line = eventStoreProcess.StandardOutput.ReadLine()
                if line <> null then
                    IntegrationTests.log.Debug (lazy line)
                if line <> null && line.Contains("SystemInit") then started <- true
            eventStoreProcess
        with | _ ->
            eventStoreProcess.Kill()
            reraise()

    let connectToEventStore () =
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
        ensureNoZombieEventStores ()
        let eventStoreProcess = startNewProcess ()
        let connection = connectToEventStore ()

        {
            Process = eventStoreProcess
            Connection = connection
        }