namespace Eventful.Testing

open Eventful
open Eventful.EventStream

open FSharpx
open FSharpx.Collections
open FSharpx.Option

module TestInterpreter =
    let log = createLogger "Eventful.Testing.TestInterpreter"

    let rec interpret 
        (prog : EventStreamProgram<'T,_>)
        (eventStore : TestEventStore<'TMetadata, 'TAggregateType>) 
        (useSnapshots : bool)
        (eventStoreTypeToClassMap : EventStoreTypeToClassMap)
        (classToEventStoreTypeMap : ClassToEventStoreTypeMap)
        (values : Map<EventToken,(obj * 'TMetadata)>) 
        (writes : Vector<string * int * EventStreamEvent<'TMetadata>>)
        : (TestEventStore<'TMetadata, 'TAggregateType> * 'T)= 
        match prog with
        | FreeEventStream (GetEventStoreTypeToClassMap ((), f)) ->
            let next = f eventStoreTypeToClassMap
            interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (GetClassToEventStoreTypeMap ((), f)) ->
            let next = f classToEventStoreTypeMap
            interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (LogMessage (logLevel, messageTemplate, args, f)) ->
            // todo take level into account
            log.RichDebug messageTemplate args
            interpret f eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (RunAsync asyncBlock) ->
            let next = asyncBlock |> Async.RunSynchronously
            interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (ReadSnapshot (streamId, typeMap, f)) ->
            let next =
                if useSnapshots then
                    eventStore.AggregateStateSnapShots
                    |> Map.tryFind streamId
                    |> Option.getOrElse StateSnapshot.Empty
                    |> f
                else
                    f StateSnapshot.Empty

            interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes 
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
                interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values' writes
            | None ->
                let next = f None
                interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (ReadValue (token, g)) ->
            let eventObj = values.[token]
            let next = g eventObj
            interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
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

                interpret (next (WriteSuccess eventStore'.Position)) eventStore' useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
            else
                interpret (next WriteResult.WrongExpectedVersion) eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | FreeEventStream (NotYetDone g) ->
            let next = g ()
            interpret next eventStore useSnapshots eventStoreTypeToClassMap classToEventStoreTypeMap values writes
        | Pure result ->
            (eventStore,result)