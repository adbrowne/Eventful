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
    PendingEvents : Queue<string * int * EventStreamEvent<'TMetadata>>
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

    let addEvent stream (streamEvent: EventStreamEvent<'TMetadata>) (store : TestEventStore<'TMetadata, 'TAggregateType>) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let eventPosition = nextPosition store.Position
        let eventNumber = streamEvents.Length
        let streamEvents' = streamEvents |> Vector.conj (eventPosition, streamEvent)
        { store with 
            Events = store.Events |> Map.add stream streamEvents'; 
            Position = eventPosition 
            PendingEvents = store.PendingEvents |> Queue.conj (stream, eventNumber, streamEvent)}

    let runHandlerForEvent (context: 'TEventContext) interpreter (eventStream, eventNumber, evt) testEventStore (EventfulEventHandler (t, evtHandler)) =
        let program = evtHandler context eventStream eventNumber evt |> Async.RunSynchronously
        interpreter program testEventStore
        |> fst

    let runEventHandlers 
        buildEventContext 
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent, 'TAggregateType>) 
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) 
        (eventStream, eventNumber, eventStreamEvent) =
        match eventStreamEvent with
        | Event { Body = evt; EventType = eventType; Metadata = metadata } ->
            let handlers = 
                handlers.EventHandlers
                |> Map.tryFind (evt.GetType().Name)
                |> function
                | Some handlers -> handlers
                | None -> []
            let context = buildEventContext metadata
            handlers |> Seq.fold (runHandlerForEvent context interpreter (eventStream, eventNumber, { Body = evt; EventType = eventType; Metadata = metadata })) testEventStore
        | _ -> testEventStore

    let getCurrentState streamId aggregateType testEventStore =
        let snapshotKey = (streamId, aggregateType)

        testEventStore.AggregateStateSnapShots
        |> Map.tryFind snapshotKey
        |> Option.getOrElse Map.empty
        
    let updateStateSnapShot 
        (streamId, streamNumber, evt : EventStreamEvent<'TMetadata>) 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>)
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        match evt with
        | Event { Body = body; EventType = eventType; Metadata = metadata } ->
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
                        let! handler = aggregateConfig.Wakeup
                        let! newTime = handler.WakeupFold.GetState state'

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
        | EventLink _ ->
            // todo work out what to do here
            testEventStore

    let runEvent buildEventContext interpreter handlers testEventStore x =
        runEventHandlers buildEventContext interpreter handlers testEventStore x
        |> updateStateSnapShot x handlers

    /// run all event handlers for produced events
    /// that have not been run yet
    let rec processPendingEvents 
        buildEventContext
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>) 
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        match testEventStore.PendingEvents with
        | Queue.Nil -> testEventStore
        | Queue.Cons (x, xs) ->
            let next = 
                runEvent buildEventContext interpreter handlers { testEventStore with PendingEvents = xs } x
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
                let! wakeupHandler = aggregate.Wakeup

                let initialState = 
                    getCurrentState w.Stream w.Type testEventStore

                let! expectedTime = wakeupHandler.WakeupFold.GetState initialState
                return 
                    if expectedTime = w.Time then
                        onTimeChange w.Time
                        interpreter (wakeupHandler.Handler w.Stream aggregate.GetUniqueId expectedTime) { testEventStore with WakeupQueue = ws }
                        |> fst
                        |> processPendingEvents buildEventContext interpreter handlers
                        |> loop
                    else
                        loop { testEventStore with WakeupQueue = ws }
            } 
            |> FSharpx.Option.getOrElse testEventStore

        loop testEventStore