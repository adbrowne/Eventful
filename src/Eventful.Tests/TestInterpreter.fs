namespace Eventful.Testing

open Eventful
open Eventful.EventStream

open FSharpx.Collections
open FSharpx.Option

module TestInterpreter =
    let rec interpret 
        prog 
        (eventStore : TestEventStore) 
        (eventTypeMap : Bimap<string, ComparableType>)
        (values : Map<EventToken,obj>) 
        (writes : Vector<string * int * EventStreamEvent>)= 
        match prog with
        | FreeEventStream (GetEventTypeMap ((), f)) ->
            let next = f eventTypeMap
            interpret next eventStore eventTypeMap values writes
        | FreeEventStream (ReadFromStream (stream, eventNumber, f)) -> 
            let readEvent = maybe {
                    let! streamEvents = eventStore.Events |> Map.tryFind stream
                    let! (position, eventStreamData) = streamEvents |> Vector.tryNth eventNumber
                    return
                        match eventStreamData with
                        | Event { Body = evt; EventType = eventType; Metadata = metadata } -> 
                            let token = 
                                {
                                    Stream = stream
                                    Number = eventNumber
                                    EventType = eventType
                                }
                            (token, evt)
                        | EventLink _ -> failwith "todo"
                }
            match readEvent with
            | Some (eventToken, evt) -> 
                let next = f (Some eventToken)
                let values' = values |> Map.add eventToken evt
                interpret next eventStore eventTypeMap values' writes
            | None ->
                let next = f None
                interpret next eventStore eventTypeMap values writes
        | FreeEventStream (ReadValue (token, g)) ->
            let eventObj = values.[token]
            let next = g eventObj
            interpret next eventStore eventTypeMap values writes
        | FreeEventStream (WriteToStream (stream, expectedValue, events, next)) ->
            let streamEvents = 
                eventStore.Events 
                |> Map.tryFind stream 
                |> FSharpx.Option.getOrElse Vector.empty
                |> Vector.map snd
            
            let lastStreamEventIndex = streamEvents.Length - 1
            let expectedValueCorrect =
                match (expectedValue, lastStreamEventIndex) with
                | (Any, _) -> true
                | (NewStream, -1) -> true
                | (AggregateVersion x, y) when x = y -> true
                | _ -> false
                
            if expectedValueCorrect then
                let streamEvents' =  
                    events 
                    |> Vector.ofSeq
                    |> Vector.append streamEvents

                let addEventToQueue queue (eventNumber, evt) =
                    queue |> Queue.conj (stream, eventNumber, evt)

                let startingEventNumber = streamEvents.Length
                let numberedEvents = 
                    Seq.zip (Seq.initInfinite ((+) startingEventNumber)) streamEvents'

                let eventStore' = 
                    streamEvents' |> Vector.fold (fun s e -> s |> TestEventStore.addEvent stream e) eventStore

                interpret (next WriteSuccess) eventStore' eventTypeMap values writes
            else
                interpret (next WrongExpectedVersion) eventStore eventTypeMap values writes
        | FreeEventStream (NotYetDone g) ->
            let next = g ()
            interpret next eventStore eventTypeMap values writes
        | Pure result ->
            (eventStore,result)