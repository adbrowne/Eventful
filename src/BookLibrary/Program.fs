open System
open Eventful
open Serilog
open Nessos.UnionArgParser
open BookLibrary

type CLIArguments =
    | RavenServer of host:string * port:int
    | RavenDatabase of string
    | EventStore of host:string * port:int
    | Create_Raven_Database
    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | RavenServer _ -> "Specify Raven Server (hostname : port)."
            | RavenDatabase _ -> "Specify Raven Database name."
            | EventStore _ -> "Specify EventStore Server (hostname : port)."
            | Create_Raven_Database -> "Create Raven database and exit"

[<EntryPoint>]
let main argv = 
    let parser = UnionArgParser.Create<CLIArguments>()
    let results = parser.Parse argv

    let createRavenDb = results.Contains <@ Create_Raven_Database @>

    if createRavenDb then
        Console.WriteLine "Creating Raven Database"
    else
        let log = new LoggerConfiguration()
        let log = log
                    .WriteTo.Seq("http://localhost:5341")
                    .WriteTo.ColoredConsole()
                    .MinimumLevel.Debug()
                    .CreateLogger()

        EventfulLog.SetLog log

        let default_eventstore_config : EventStoreConfig = {
            Server = "localhost"
            TcpPort = 1113
            Username = "admin"
            Password = "changeit" }

        let default_raven_config : RavenConfig = {
            Server = "localhost"
            Port = 8080
            Database = "BookLibrary"
        }

        let default_web_config : WebServerConfig = {
            Server = "localhost"
            Port = 8083
        }

        let default_application_config : ApplicationConfig = {
            Raven = default_raven_config
            EventStore = default_eventstore_config
            WebServer = default_web_config 
        }

        let applicationConfig = 
            let applyArgument (config : ApplicationConfig) = function
                | RavenServer (host, port) ->
                     { config with Raven = { config.Raven with Server = host; Port = port }}
                | RavenDatabase database -> 
                     { config with Raven = { config.Raven with Database = database }}
                | EventStore (host, port) ->
                     { config with EventStore = { config.EventStore with Server = host; TcpPort = port }}
                | x -> failwith <| sprintf "Unhandled argument %A" x

            results.GetAllResults()
            |> List.fold applyArgument default_application_config

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