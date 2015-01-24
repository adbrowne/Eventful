namespace Eventful.Tests.Integration

open System
open System.IO
open System.Diagnostics
open Raven.Client
open Raven.Client.Extensions
open Eventful

module InMemoryRavenRunner = 
    let logger = createLogger "Eventful.Tests.Integration.InMemoryRavenRunner"
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

        IntegrationTests.logOutput logger ravenProcess

        {
            Process = ravenProcess
            HttpPort = ravenPort
        }