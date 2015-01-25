namespace Eventful.Tests.Integration

open Eventful
open Serilog
open Xunit
open HttpClient
open FSharpx
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

    let withProxy request =
        request
        |> HttpClient.withProxy { Proxy.Address = "127.0.0.1"; Proxy.Credentials = ProxyCredentials.None; Port = 8888 }

    let createBook apiBaseUrl title = 
        let createUrl = (sprintf "%sbooks" apiBaseUrl) 

        let getBookId (response : Response) = 
            response.EntityBody
            |> Option.map (fun x -> (JsonValue.Parse(x)?bookId).AsGuid())
            
        HttpClient.createRequest Post createUrl
        |> withProxy
        |> HttpClient.withBody """{ "title": "Test Book" }"""
        |> HttpClient.getResponseAsync
        |> Async.map getBookId

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

            log.Debug("{@RavenPort}", [|ravenProcess.HttpPort|])
            let apiBaseUrl = sprintf "http://ipv4.fiddler:%d/api/" bookLibraryProcess.HttpPort

            let! bookIds = 
                 [1..1]
                 |> List.map (sprintf "Book %d")
                 |> List.map (createBook apiBaseUrl)
                 |> Async.Parallel
                 |> Async.map List.ofArray

            test <@ match bookIds with
                    | [bookId] ->
                        printfn "BookId: %A" bookId
                        true
                    | _ -> false @>  

            ()
        } |> Async.StartAsTask