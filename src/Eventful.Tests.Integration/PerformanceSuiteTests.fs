namespace Eventful.Tests.Integration

open Eventful
open Serilog
open Xunit
open HttpClient
open FSharpx.Choice
open Swensen.Unquote
open FSharp.Data
open FSharp.Data.JsonExtensions
open System.Threading.Tasks

module PerformanceSuiteTests =

    let toChoice err a =
        match a with
        | Some v -> Choice1Of2 v
        | None -> Choice2Of2 err

    [<Fact>]
    let ``Full Integration Run`` () : Task<unit> = 
        async {

            let log = new LoggerConfiguration()
            let log = log
                        .WriteTo.Seq("http://localhost:5341")
                        .WriteTo.ColoredConsole()
                        .MinimumLevel.Debug()
                        .CreateLogger()

            EventfulLog.SetLog log
            use eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore()
            use ravenProcess = InMemoryRavenRunner.startNewProcess()

            let config = { 
                BookLibrary.ApplicationConfig.default_application_config with 
                    Raven = { BookLibrary.ApplicationConfig.default_raven_config with Port = ravenProcess.HttpPort }
                    EventStore = { BookLibrary.ApplicationConfig.default_eventstore_config with TcpPort = eventStoreProcess.TcpPort }}
            BookLibraryRunner.setupDatabase config
            use bookLibraryProcess = BookLibraryRunner.startNewProcess config

            let apiBaseUrl = sprintf "http://localhost:%d/api/" bookLibraryProcess.HttpPort

            let createUrl = (sprintf "%sbooks" apiBaseUrl) 

            let createBookResult = 
                HttpClient.createRequest Post createUrl
                // |> HttpClient.withProxy { Proxy.Address = "localhost"; Proxy.Credentials = ProxyCredentials.None; Port = 8888 }
                |> HttpClient.withBody """{ "title": "Test Book" }"""
                |> HttpClient.getResponse
                |> (fun x -> x.EntityBody)
                |> Option.map (fun x -> (JsonValue.Parse(x)?bookId).AsGuid())
                |> toChoice "No Body" 

            test <@ match createBookResult with
                    | Choice1Of2 bookId ->
                        printfn "BookId: %A" bookId
                        true
                    | _ -> false @>  

        } |> Async.StartAsTask