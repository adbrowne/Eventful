namespace Eventful.Tests.Integration

open System
open System.IO
open ICSharpCode.SharpZipLib.Zip
open ICSharpCode.SharpZipLib.Core
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


    let eventStoreDirectory = ".\EventStore3"
    let installCompleteMarkerFile = Path.Combine(eventStoreDirectory, "test_setup.mrk")
    let clusterNodeExecutable = Path.Combine(eventStoreDirectory, "EventStore.ClusterNode.exe")
    let testClusterNodeProcessName = "EventStore.ClusterNode.Test"
    let testClusterNodeExecutable = Path.Combine(eventStoreDirectory, testClusterNodeProcessName + ".exe")
    let windowsEventStoreUri = "http://download.geteventstore.com/binaries/EventStore-OSS-Win-v3.0.0.zip"
    let testTcpPort = 11130
    let testHttpPort = 21130

    // adapted from https://github.com/icsharpcode/SharpZipLib/wiki/Zip-Samples#-unpack-a-zip-using-zipinputstream-eg-for-unseekable-input-streams
    let unzipFromStream (zipStream:Stream) (outFolder : string) =
        let zipInputStream = new ZipInputStream(zipStream)
        let zipEntry = ref (zipInputStream.GetNextEntry())
        while (!zipEntry <> null) do
            let entryFileName = (!zipEntry).Name
            // to remove the folder from the entry:- entryFileName = Path.GetFileName(entryFileName);
            // Optionally match entrynames against a selection list here to skip as desired.
            // The unpacked length is available in the zipEntry.Size property.

            let buffer = Array.zeroCreate<byte> 4096;     // 4K is optimum

            // Manipulate the output filename here as desired.
            let fullZipToPath = Path.Combine(outFolder, entryFileName)
            let directoryName = Path.GetDirectoryName(fullZipToPath)
            if (directoryName.Length > 0) then
                Directory.CreateDirectory(directoryName) |> ignore

            // Unzip file in buffered chunks. This is just as fast as unpacking to a buffer the full size
            // of the file, but does not waste memory.
            // The "using" will close the stream even if an exception occurs.
            if (!zipEntry).IsFile then
                use streamWriter = File.Create(fullZipToPath)
                StreamUtils.Copy(zipInputStream, streamWriter, buffer)

            zipEntry := (zipInputStream.GetNextEntry())

    // this ensures that when we shutdown processes with this
    // name we don't shutdown any normal cluster nodes
    // on the machine
    let makeTestExecutableCopy () =
        File.Copy(clusterNodeExecutable, testClusterNodeExecutable)

    // funny pun below
    let getEventStore () =
        if Directory.Exists(eventStoreDirectory) then
            Directory.Delete(eventStoreDirectory, true)

        let webClient = new System.Net.WebClient()
        use readStream = webClient.OpenRead(windowsEventStoreUri)
        unzipFromStream readStream eventStoreDirectory
        makeTestExecutableCopy()

    let ensureEventStoreExists () =
        let exists = File.Exists(installCompleteMarkerFile)

        if not exists then getEventStore()

        File.WriteAllText(installCompleteMarkerFile, "Complete")

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
        ensureEventStoreExists () 
        let eventStoreProcess = startNewProcess ()
        let connection = connectToEventStore ()

        {
            Process = eventStoreProcess
            Connection = connection
        }