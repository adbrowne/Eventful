namespace Eventful.EventStore 

open Eventful
open Eventful.EventStream
open FSharpx.Collections
open FSharpx.Option
open EventStore.ClientAPI

module EventStreamInterpreter = 
    let interpret<'A> (eventStore : Client) (serializer : ISerializer) prog  : Async<'A> = 
        let rec loop prog (values : Map<EventToken,(byte[]*byte[])>) (writes : Vector<string * int * obj * EventMetadata>) : Async<'A> =
            match prog with
            | FreeEventStream (ReadFromStream (stream, eventNumber, f)) -> 
                async {
                    let! event = eventStore.readEvent stream eventNumber
                    let readEvent = 
                        if(event.Status = EventReadStatus.Success) then
                            let event = event.Event.Value.Event
                            let eventToken = {
                                Stream = stream
                                Number = eventNumber
                                EventType = event.EventType
                            }
                            Some (eventToken, (event.Data, event.Metadata))
                        else
                            None

                    match readEvent with
                    | Some (eventToken, evt) -> 
                        let next = f (Some eventToken)
                        let values' = values |> Map.add eventToken evt
                        return! loop next values' writes
                    | None ->
                        let next = f None
                        return! loop next values writes
                }
            | FreeEventStream (ReadValue (token, eventType, g)) ->
                let (data, metadata) = values.[token]
                let dataObj = serializer.DeserializeObj(data) token.EventType
                let next = g dataObj
                loop next  values writes
            | FreeEventStream (WriteToStream (stream, expectedValue, data, metadata, next)) ->
                let writes' = writes |> Vector.conj (stream, expectedValue, data, metadata)
                loop next values writes'
            | FreeEventStream (NotYetDone g) ->
                let next = g ()
                loop next values writes
            | Pure result ->
                async {
                    for (streamId, eventNumber, dataObj, metadata) in writes do
                        let serializedData = serializer.Serialize(dataObj)
                        let serializedMetadata = serializer.Serialize(metadata)
                        let typeString = dataObj.GetType().FullName
                        let eventData = new EventData(System.Guid.NewGuid(), typeString, true, serializedData, serializedMetadata) 
                        do! eventStore.append streamId eventNumber [|eventData|]
                    return result
                }
        loop prog Map.empty Vector.empty