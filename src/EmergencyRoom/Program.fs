open EmergencyRoom
open System
open Topshelf
open Topshelf.HostConfigurators

[<EntryPoint>]
let main argv = 
    let runService () =
        EmergencyRoomTopShelfService()
        |> fun svc -> serviceControl (fun hc -> svc.Start() |> ignore ; true) (fun hc -> svc.Stop() ; true)

    configureTopShelf <| fun conf ->
        conf |> dependsOnIIS
        conf |> runAsLocalSystem
        conf |> startAutomatically
     
        "EmergencyRoom App" |> description conf
        "EmergencyRoom.App" |> serviceName conf
        "EmergencyRoom App" |> instanceName conf
     
        runService |> service conf