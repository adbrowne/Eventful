namespace Eventful.Tests.Integration

open System
open System.IO
open System.Diagnostics
open Eventful
open BookLibrary

module BookLibraryRunner =
    let logger = createLogger "Eventful.Tests.Integration.BookLibraryRunner"
    type BookLibraryAccess =     
        { Process : Process
          HttpPort : int }
        interface IDisposable with
         member this.Dispose() =
             this.Process.Kill()
             this.Process.WaitForExit()
             this.Process.Dispose()

    let private executableAbsolutePath = Path.Combine(IntegrationTests.bookLibraryExecutableDirectory, "BookLibrary.exe")

    let commonArgs (applicationConfig : BookLibrary.ApplicationConfig) = seq {
            yield CLIArguments.EventStore (applicationConfig.EventStore.Server, applicationConfig.EventStore.TcpPort)
            yield CLIArguments.RavenServer (applicationConfig.Raven.Server, applicationConfig.Raven.Port)
            yield CLIArguments.RavenDatabase (applicationConfig.Raven.Database)
        }
        
    let setupDatabase (applicationConfig : BookLibrary.ApplicationConfig) =
        let processArguments = 
            seq {
                yield! commonArgs applicationConfig
                yield CLIArguments.Create_Raven_Database
            }
            |> List.ofSeq
            |> CLIArguments.Parser.PrintCommandLineFlat 

        let bookLibraryProcess = IntegrationTests.startProcess executableAbsolutePath processArguments
        bookLibraryProcess.WaitForExit()

    let dataReceived blah =
        ()

    let startNewProcess (applicationConfig : BookLibrary.ApplicationConfig) =
        let httpPort = IntegrationTests.findFreeTcpPort()

        let processArguments = 
            seq {
                yield! commonArgs applicationConfig
                yield CLIArguments.WebServer ("localhost", httpPort)
            }
            |> List.ofSeq
            |> CLIArguments.Parser.PrintCommandLineFlat 

        let bookLibraryProcess = IntegrationTests.startProcess executableAbsolutePath processArguments

        try
            let mutable started = false

            while not started do
                let line = bookLibraryProcess.StandardOutput.ReadLine()
                if line <> null then
                    IntegrationTests.log.Debug (lazy line)
                if line <> null && line.Contains("Press 'q' to exit") then started <- true

            IntegrationTests.logOutput logger bookLibraryProcess

            {
                Process = bookLibraryProcess
                HttpPort = httpPort
            }
        with | _ ->
            bookLibraryProcess.Kill()
            reraise()

        
