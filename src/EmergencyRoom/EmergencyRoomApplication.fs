namespace EmergencyRoom

open System
open System.IO
open Eventful
open EventStore.ClientAPI
open Eventful.EventStore

type EmergencyRoomTopShelfService () =
    let log = createLogger "EmergencyRoom.EmergencyRoomTopShelfService"

    let mutable client : Client option = None

    let mutable eventStoreSystem : EventStoreSystem<unit,unit,EmergencyEventMetadata> option = None

    member x.Start () =
        log.Debug <| lazy "Starting App"
        async {
            let! connection = EmergencyRoomApplicationConfig.getConnection()
            let c = new Client(connection)

            let system = EmergencyRoomApplicationConfig.buildEventStoreSystem c

            client <- Some c
            eventStoreSystem <- Some system
        } |> Async.StartAsTask

    member x.Stop () =
        log.Debug <| lazy "App Stopping"