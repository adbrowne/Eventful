namespace Eventful.EventStore

open System
open System.Text
open FSharp.Data
open EventStore.ClientAPI

module ProcessingTracker = 
    let positionStream = "EventStoreProcessPosition"
    let readPosition (client : Client) = async {
        let! position = client.readStreamHead positionStream
        match position with
        | Some ev ->  
            let body = 
                Encoding.UTF8.GetString(ev.Event.Data)            
                |> JsonValue.Parse

            match body with
            | JsonValue.Record [| 
                                 "commitPosition", JsonValue.Number commitPosition 
                                 "preparePosition", JsonValue.Number preparePosition |] -> 
                return Some (new Position(int64 commitPosition, int64 preparePosition))
            | _ -> return raise (new Exception(sprintf "malformed position metadata %s" positionStream))
        | None -> return None
    }

    let setPosition (client : Client) (position : Position) = async {
        let jsonBytes =
            [|  
                ("commitPosition", JsonValue.Number (decimal position.CommitPosition))
                ("preparePosition", JsonValue.Number (decimal position.PreparePosition))
            |]
            |> FSharp.Data.JsonValue.Record
            |> (fun x -> x.ToString())
            |> Encoding.UTF8.GetBytes
        let eventData = new EventData(Guid.NewGuid(), "ProcessPosition", true, jsonBytes, null)
        do! client.append positionStream ExpectedVersion.Any [|eventData|] |> Async.Ignore
        let streamMetadata = EventStore.ClientAPI.StreamMetadata.Create(Nullable(1))
        do! client.ensureMetadata positionStream  streamMetadata
    }