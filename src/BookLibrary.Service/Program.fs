open System
open Topshelf
open Topshelf.HostConfigurators

[<EntryPoint>]
let main argv = 
    let runService () =
        BookLibrary.TopShelfService()
        |> fun svc -> serviceControl (fun hc -> svc.Start() |> ignore ; true) (fun hc -> svc.Stop() ; true)

    configureTopShelf <| fun conf ->
        conf |> dependsOnIIS
        conf |> runAsLocalSystem
        conf |> startAutomatically
     
        "BookLibrary App" |> description conf
        "BookLibrary.App" |> serviceName conf
        "BookLibrary App" |> instanceName conf
     
        runService |> service conf