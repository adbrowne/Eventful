open System
open Topshelf
open Topshelf.HostConfigurators
open Eventful
open Serilog

[<EntryPoint>]
let main argv = 

    let log = new LoggerConfiguration()
    let log = log
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.ColoredConsole()
                .MinimumLevel.Debug()
                .CreateLogger()

    EventfulLog.SetLog log

    let runService () =
        BookLibrary.TopShelfService()
        |> fun svc -> serviceControl (fun hc -> svc.Start() |> ignore ; true) (fun hc -> svc.Stop() ; true)

    configureTopShelf <| fun conf ->
        conf |> dependsOnIIS
        conf |> runAsLocalSystem
        conf |> startAutomatically
        conf |> useSerilog
     
        "BookLibrary App" |> description conf
        "BookLibrary.App" |> serviceName conf
        "BookLibrary App" |> instanceName conf
     
        runService |> service conf