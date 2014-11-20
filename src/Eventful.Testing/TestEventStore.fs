namespace Eventful.Testing

open FSharpx.Collections
open FSharpx.Option
open FSharpx
open Eventful
open System

type WakeupRecord<'TAggregateType> = {
    Time : DateTime
    Stream: string
    Type : 'TAggregateType
}

type TestEventStore<'TMetadata, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison> = {
    Position : EventPosition
    Events : Map<string,Vector<EventPosition * EventStreamEvent<'TMetadata>>>
    PendingEvents : Queue<PersistedStreamEntry<'TMetadata>>
    AggregateStateSnapShots : Map<(string * 'TAggregateType), Map<string,obj>>
    WakeupQueue : IPriorityQueue<WakeupRecord<'TAggregateType>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let nextPosition (position : EventPosition) = 
        {
            Commit = position.Commit + 1L
            Prepare = position.Commit + 1L
        }

    let empty<'TMetadata, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison> : TestEventStore<'TMetadata, 'TAggregateType> = { 
        Position = EventPosition.Start
        Events = Map.empty
        PendingEvents = Queue.empty 
        AggregateStateSnapShots = Map.empty
        WakeupQueue = PriorityQueue.empty false }

    let tryGetEvent (store : TestEventStore<'TMetadata, 'TAggregateType>) streamId eventNumber =
        maybe {
            let! stream = store.Events |> Map.tryFind streamId
            let! (_, entry) = stream |> Vector.tryNth eventNumber
            return! match entry with 
                    | Event evt -> Some evt 
                    | _ -> None
        }

    let addEvent stream (streamEvent: EventStreamEvent<'TMetadata>) (store : TestEventStore<'TMetadata, 'TAggregateType>) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let eventId = Guid.NewGuid()

        let eventPosition = nextPosition store.Position
        let eventNumber = streamEvents.Length
        let streamEvents' = streamEvents |> Vector.conj (eventPosition, streamEvent)

        let persistedStreamEntry = 
            match streamEvent with
            | Event evt ->
                PersistedStreamEvent {
                    StreamId = stream
                    EventNumber = eventNumber
                    EventId = eventId
                    Body = evt.Body
                    EventType = evt.EventType
                    Metadata = evt.Metadata
                }
            | EventLink (linkedStreamId, linkedEventNumber, metadata) ->
                match tryGetEvent store linkedStreamId linkedEventNumber with
                | Some linkedEvent -> 
                    PersistedStreamLink {
                        StreamId = stream
                        EventNumber = eventNumber
                        EventId = eventId
                        LinkedStreamId = linkedStreamId
                        LinkedEventNumber = linkedEventNumber
                        LinkedBody = linkedEvent.Body
                        LinkedEventType = linkedEvent.EventType
                        LinkedMetadata = linkedEvent.Metadata
                    }
                | None ->
                    failwith "Could not find linked event"
        { store with 
            Events = store.Events |> Map.add stream streamEvents'; 
            Position = eventPosition 
            PendingEvents = store.PendingEvents |> Queue.conj persistedStreamEntry}

    let runHandlerForEvent interpreter testEventStore program  =
        let program = program |> Async.RunSynchronously
        interpreter program testEventStore
        |> fst

    let runEventHandlers 
        buildEventContext 
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent, 'TAggregateType>) 
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) 
        (persistedEvent : PersistedEvent<'TMetadata>) =
            let handlerPrograms = 
                EventfulHandlers.getHandlerPrograms buildEventContext persistedEvent handlers
            handlerPrograms |> Seq.fold (runHandlerForEvent interpreter) testEventStore

    let getCurrentState streamId aggregateType testEventStore =
        let snapshotKey = (streamId, aggregateType)

        testEventStore.AggregateStateSnapShots
        |> Map.tryFind snapshotKey
        |> Option.getOrElse Map.empty
        
    let applyEventDataToSnapshot 
        streamId
        body
        metadata
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>)
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        let aggregateType = handlers.GetAggregateType metadata
        match handlers.AggregateTypes |> Map.tryFind aggregateType with
        | Some aggregateConfig ->
            let snapshotKey = (streamId, aggregateType)

            let initialState = 
                getCurrentState streamId aggregateType testEventStore

            let state' = 
                initialState
                |> AggregateStateBuilder.dynamicRun aggregateConfig.StateBuilder.GetBlockBuilders () body metadata 

            let snapshots' = testEventStore.AggregateStateSnapShots |> Map.add snapshotKey state'

            let wakeupQueue' =
                FSharpx.Option.maybe {
                    let! EventfulWakeupHandler(wakeupFold, _) = aggregateConfig.Wakeup
                    let! newTime = wakeupFold.GetState state'

                    let newWakeupRecord = {
                        Time = newTime
                        Stream = streamId
                        Type = aggregateType
                    }
                    return 
                        testEventStore.WakeupQueue |> PriorityQueue.insert newWakeupRecord }
                |> FSharpx.Option.getOrElse testEventStore.WakeupQueue

            { testEventStore with
                AggregateStateSnapShots = snapshots'
                WakeupQueue = wakeupQueue' }
        | None -> 
            testEventStore
        
    let updateStateSnapShot 
        (persistedStreamEntry : PersistedStreamEntry<'TMetadata>) 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>)
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        match persistedStreamEntry with
        | PersistedStreamEvent { Body = body; Metadata = metadata; StreamId = streamId } ->
            applyEventDataToSnapshot streamId body metadata handlers testEventStore
        | PersistedStreamLink  { LinkedBody = body; LinkedMetadata = metadata; LinkedStreamId = streamId } ->
            applyEventDataToSnapshot streamId body metadata handlers testEventStore

    let runEvent buildEventContext interpreter handlers testEventStore streamEntry =
        match streamEntry with
        | PersistedStreamEvent persistedEvent ->
            runEventHandlers 
                buildEventContext 
                interpreter 
                handlers 
                testEventStore 
                persistedEvent
        | PersistedStreamLink _ -> 
           testEventStore

    /// run all event handlers for produced events
    /// that have not been run yet
    let rec processPendingEvents 
        buildEventContext
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>) 
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        match testEventStore.PendingEvents with
        | Queue.Nil -> testEventStore
        | Queue.Cons (streamEntry, xs) ->
            let next = 
                runEvent buildEventContext interpreter handlers { testEventStore with PendingEvents = xs } streamEntry
                |> updateStateSnapShot streamEntry handlers
            processPendingEvents buildEventContext interpreter handlers next

    let runCommand interpreter cmd handler testEventStore =
        let program = handler cmd 
        interpreter program testEventStore

    let rec runToEnd 
        onTimeChange
        buildEventContext
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>) 
        (testEventStore :  TestEventStore<'TMetadata, 'TAggregateType>)
        : TestEventStore<'TMetadata, 'TAggregateType> =

        let rec loop testEventStore =
            maybe {
                let! (w,ws) =  testEventStore.WakeupQueue |> PriorityQueue.tryPop 
                let! aggregate = handlers.AggregateTypes |> Map.tryFind w.Type
                let! EventfulWakeupHandler(wakeupFold, wakeupHandler) = aggregate.Wakeup

                let initialState = 
                    getCurrentState w.Stream w.Type testEventStore

                let! expectedTime = wakeupFold.GetState initialState
                return 
                    if expectedTime = w.Time then
                        onTimeChange w.Time
                        interpreter (wakeupHandler w.Stream expectedTime) { testEventStore with WakeupQueue = ws }
                        |> fst
                        |> processPendingEvents buildEventContext interpreter handlers
                        |> loop
                    else
                        loop { testEventStore with WakeupQueue = ws }
            } 
            |> FSharpx.Option.getOrElse testEventStore

        loop testEventStore