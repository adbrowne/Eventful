namespace Eventful.Tests.Integration

open System
open System.IO
open System.Diagnostics
open Raven.Client
open Raven.Client.Extensions

module InMemoryRavenRunner = 
    type RavenAccess =     
        { Process : Process
          HttpPort : int }
        interface IDisposable with
         member this.Dispose() =
             this.Process.Kill()
             this.Process.WaitForExit()
             this.Process.Dispose()

    let private executableAbsolutePath = Path.Combine(IntegrationTests.buildDirectoryPath, "RavenDb\Server\Raven.Server.exe")

    let startNewProcess () =
        let ravenPort = IntegrationTests.findFreeTcpPort()
        let processArguments = sprintf "-ram --set=Raven/Port==%d" ravenPort

        let ravenProcess = IntegrationTests.startProcess executableAbsolutePath processArguments

        {
            Process = ravenProcess
            HttpPort = ravenPort
        }