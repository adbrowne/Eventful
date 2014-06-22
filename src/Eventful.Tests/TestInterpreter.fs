namespace Eventful.Testing

open Eventful
open Eventful.EventStream

open FSharpx.Collections
open FSharpx.Option

module TestInterpreter =
    let rec interpret prog (eventStore : TestEventStore) (values : Map<EventToken,obj>) (writes : Vector<string * int * EventStreamEvent>)= 
        match prog with
        | FreeEventStream (ReadFromStream (stream, eventNumber, f)) -> 
            let readEvent = maybe {
                    let! streamEvents = eventStore.Events |> Map.tryFind stream
                    let! eventStreamData = streamEvents |> Vector.tryNth eventNumber
                    return
                        match eventStreamData with
                        | Event (evt, _) -> 
                            let token = 
                                {
                                    Stream = stream
                                    Number = eventNumber
                                    EventType = evt.GetType().Name
                                }
                            (token, evt)
                        | EventLink _ -> failwith "todo"
                }

            match readEvent with
            | Some (eventToken, evt) -> 
                let next = f (Some eventToken)
                let values' = values |> Map.add eventToken evt
                interpret next eventStore values' writes
            | None ->
                let next = f None
                interpret next eventStore values writes
        | FreeEventStream (ReadValue (token, eventType, g)) ->
            let eventObj = values.[token]
            let next = g eventObj
            interpret next eventStore values writes
        | FreeEventStream (WriteToStream (stream, expectedValue, events, next)) ->
            let addEvent w evnetStreamData = 
                w |> Vector.conj (stream, expectedValue, evnetStreamData) 
            let writes' = Seq.fold addEvent writes events
            interpret (next WriteSuccess) eventStore values writes'
        | FreeEventStream (NotYetDone g) ->
            let next = g ()
            interpret next eventStore values writes
        | Pure result ->
            let writeEvent store (stream, exepectedValue, eventStreamData) =
                // todo check expected value
                let streamEvents = 
                    store.Events 
                    |> Map.tryFind stream 
                    |> FSharpx.Option.getOrElse Vector.empty
                    |> Vector.conj eventStreamData
                
                { store with Events = store.Events |> Map.add stream streamEvents }

            let eventStore' = 
                writes |> Vector.fold writeEvent eventStore
            (eventStore',result)