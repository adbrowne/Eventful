namespace Eventful.EventStore

open System
open System.Text
open FSharp.Data
open EventStore.ClientAPI
open Eventful

module ProcessingTracker = 
    let private deserializePosition (data : byte[]) =
        let body = 
            Encoding.UTF8.GetString(data)
            |> JsonValue.Parse

        match body with
        | JsonValue.Record [| 
                             "commitPosition", JsonValue.Number commitPosition 
                             "preparePosition", JsonValue.Number preparePosition |] -> 
            { EventPosition.Commit = int64 commitPosition
              Prepare = int64 preparePosition}
        | _ -> raise (new Exception(sprintf "malformed position metadata"))

    let private serializePosition (position : EventPosition) = 
        [|  
            ("commitPosition", JsonValue.Number (decimal position.Commit))
            ("preparePosition", JsonValue.Number (decimal position.Prepare))
        |]
        |> FSharp.Data.JsonValue.Record
        |> (fun x -> x.ToString())
        |> Encoding.UTF8.GetBytes

    let ensureTrackingStreamMetadata (client : Client) streamId = async {
        let! existingMetadata = client.getStreamMetadata streamId
        if (existingMetadata.StreamMetadata.MaxCount <> Nullable(1)) then
            let streamMetadata = EventStore.ClientAPI.StreamMetadata.Create(Nullable(1))
            do! client.writeStreamMetadata streamId streamMetadata
    }

    let ensureTrackingStreamMetadataAsync (client : Client) streamId =
        ensureTrackingStreamMetadata client streamId |> Async.StartAsTask
        
    let readPosition (client : Client) streamId = async {
        let! (position, _) = client.readStreamHead streamId
        return 
            match position with
            | Some ev ->  
                deserializePosition ev.Event.Data
            | None -> EventPosition.Start
    }

    let readPositionAsync client streamId =
        readPosition client streamId |> Async.StartAsTask

    let setPosition (client : Client) streamId (position : EventPosition) = async {
        let jsonBytes = serializePosition position
        let eventData = new EventData(Guid.NewGuid(), "ProcessPosition", true, jsonBytes, null)
        let! writeResult = client.append streamId ExpectedVersion.Any [|eventData|]

        return
            match writeResult with
            | WriteSuccess _ -> ()
            | WriteResult.WriteCancelled ->
                failwith "EventStore position write cancelled"
            | WriteResult.WriteError exn ->
                failwith <| sprintf "Exception writing EventStore position %A" exn
            | WriteResult.WrongExpectedVersion ->
                failwith <| sprintf "Wrong Version writing EventStore position"
    }

    let setPositionAsync client streamId position =
        setPosition client streamId position
        |> Async.StartAsTask