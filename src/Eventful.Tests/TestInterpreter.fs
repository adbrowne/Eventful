namespace Eventful.Testing

open Eventful
open Eventful.EventStream

open FSharpx.Collections
open FSharpx.Option

module TestInterpreter =
    let rec interpret 
        prog 
        (eventStore : TestEventStore<'TMetadata>) 
        (eventStoreTypeToClassMap : EventStoreTypeToClassMap)
        (classToEventStoreTypeMap : ClassToEventStoreTypeMap)
        (values : Map<EventToken,(obj * 'TMetadata)>) 
        (writes : Vector<string * int * EventStreamEvent<'TMetadata>>)= 
        match prog with
        | FreeEventStream (GetEventStoreTypeToClassMap ((), f)) ->
            let next = f eventStoreTypeToClassMap
            interpret next eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (GetClassToEventStoreTypeMap ((), f)) ->
            let next = f classToEventStoreTypeMap
            interpret next eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values writes
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
                            (token, evt, metadata)
                        | EventLink _ -> failwith "todo"
                }
            match readEvent with
            | Some (eventToken, evt, metadata) -> 
                let next = f (Some eventToken)
                let values' = values |> Map.add eventToken (evt,metadata)
                interpret next eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values' writes
            | None ->
                let next = f None
                interpret next eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (ReadValue (token, g)) ->
            let eventObj = values.[token]
            let next = g eventObj
            interpret next eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values writes
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
                let eventStore' = 
                    events |> Vector.ofSeq |> Vector.fold (fun s e -> s |> TestEventStore.addEvent stream e) eventStore

                interpret (next (WriteSuccess eventStore'.Position)) eventStore' eventStoreTypeToClassMap classToEventStoreTypeMap values writes
            else
                interpret (next WriteResult.WrongExpectedVersion) eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (NotYetDone g) ->
            let next = g ()
            interpret next eventStore eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | Pure result ->
            (eventStore,result)