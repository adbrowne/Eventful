open System
open Eventful
open Serilog
open BookLibrary
open Raven.Client.Extensions

[<EntryPoint>]
let main argv = 
    let log = new LoggerConfiguration()
    let log = log
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.ColoredConsole()
                .MinimumLevel.Debug()
                .CreateLogger()

    EventfulLog.SetLog log

    let results = CLIArguments.Parser.Parse argv

    let createRavenDb = results.Contains <@ Create_Raven_Database @>

    let applicationConfig = 
        let applyArgument (config : ApplicationConfig) = function
            | RavenServer (host, port) ->
                 { config with Raven = { config.Raven with Server = host; Port = port }}
            | RavenDatabase database -> 
                 { config with Raven = { config.Raven with Database = database }}
            | EventStore (host, port) ->
                 { config with EventStore = { config.EventStore with Server = host; TcpPort = port }}
            | WebServer (host, port) ->
                 { config with WebServer = { config.WebServer with Server = host; Port = port }}
            | Create_Raven_Database -> config

        results.GetAllResults()
        |> List.fold applyArgument ApplicationConfig.default_application_config

    if createRavenDb then
        let ravenConfig = applicationConfig.Raven
        Console.WriteLine "Creating Raven Database"
        let documentStore = SetupHelpers.buildDocumentStore ravenConfig
        documentStore.DatabaseCommands.EnsureDatabaseExists(ravenConfig.Database)
        let definition = Eventful.Raven.AggregateStatePersistence.wakeupIndex()
        documentStore.DatabaseCommands.ForDatabase(ravenConfig.Database).PutIndex(definition.Name, definition, true) |> ignore
    else
        let runner = new BookLibrary.BookLibraryServiceRunner(applicationConfig)

        runner.Start() |> ignore

        Console.WriteLine("Press 'q' to exit")
        let rec waitForExit () =
            let key = Console.ReadKey()
            match key.KeyChar with
            | 'q' -> ()
            | 'Q' -> ()
            | _ -> waitForExit ()

        waitForExit()

    0