namespace Eventful.Testing

open FSharpx.Collections
open FSharpx.Option
open FSharpx
open Eventful
open System
open FSharp.Control

type WakeupRecord = {
    Time : UtcDateTime
    Stream: string
    Type : string
}

type TestEventStore<'TMetadata when 'TMetadata : equality> = {
    Position : EventPosition
    Events : Map<string,Vector<EventPosition * EventStreamEvent<'TMetadata>>>
    StreamMetadata : Map<string,Vector<EventStreamMetadata>>
    PendingEvents : Queue<(int * PersistedStreamEntry<'TMetadata>)>
    AggregateStateSnapShots : Map<string, StateSnapshot>
    WakeupQueue : IPriorityQueue<WakeupRecord>
}

type IInterpreter<'TMetadata when 'TMetadata : equality> = 
    abstract member Run<'A> : EventStream.EventStreamProgram<'A,'TMetadata> -> TestEventStore<'TMetadata> -> (TestEventStore<'TMetadata> * 'A)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let nextPosition (position : EventPosition) = 
        {
            Commit = position.Commit + 1L
            Prepare = position.Commit + 1L
        }

    let empty<'TMetadata, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison> : TestEventStore<'TMetadata> = { 
        Position = EventPosition.Start
        Events = Map.empty
        StreamMetadata = Map.empty
        PendingEvents = Queue.empty 
        AggregateStateSnapShots = Map.empty
        WakeupQueue = PriorityQueue.empty false }

    let getStreamMetadata streamId (store : TestEventStore<'TMetadata>) =
        store.StreamMetadata
        |> Map.tryFind streamId
        |> Option.bind Vector.tryLast
        |> Option.getOrElse EventStreamMetadata.Default

    let getMinimumEventNumber (streamMetadata : EventStreamMetadata) (stream : Vector<'a>) =
        match streamMetadata.MaxCount with
        | Some maxCount when stream.Length > maxCount -> stream.Length - maxCount
        | _ -> 0

    let getLastEventNumber streamId (store : TestEventStore<'TMetadata>) =
        let streamEvents = 
            store.Events 
            |> Map.tryFind streamId
            |> FSharpx.Option.getOrElse Vector.empty
            |> Vector.map snd
            
        streamEvents.Length - 1

    let getAllEvents (store : TestEventStore<'TMetadata>) streamId =
        let streamMetadata = store |> getStreamMetadata streamId

        let fullStream =
            store.Events
            |> Map.tryFind streamId
            |> Option.getOrElse Vector.empty
            |> Vector.map snd

        let minimumEventNumber = getMinimumEventNumber streamMetadata fullStream

        if minimumEventNumber = 0 then
            fullStream
        else
            fullStream |> Seq.skip minimumEventNumber |> Vector.ofSeq

    let tryGetEvent (store : TestEventStore<'TMetadata>) streamId eventNumber =
        maybe {
            let! stream = store.Events |> Map.tryFind streamId

            let streamMetadata = store |> getStreamMetadata streamId
            let minimumEventNumber = getMinimumEventNumber streamMetadata stream
            if eventNumber < minimumEventNumber then return! None else

            let! (_, entry) = stream |> Vector.tryNth eventNumber
            return entry
        }

    let addEvent stream (streamEvent: EventStreamEvent<'TMetadata>) (store : TestEventStore<'TMetadata>) =
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
                | Some (Event linkedEvent) -> 
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
                | Some (EventLink _) ->
                    failwith "Linking to a link is not supported"
                | None ->
                    failwith "Could not find linked event"
        { store with 
            Events = store.Events |> Map.add stream streamEvents'; 
            Position = eventPosition 
            PendingEvents = store.PendingEvents |> Queue.conj (eventNumber, persistedStreamEntry)}

    let runHandlerForEvent persistedEvent buildEventContext interpreter testEventStore program  =
        use context = buildEventContext persistedEvent
        let program = program context |> Async.RunSynchronously
        interpreter program testEventStore
        |> fst

    let runEventHandlers 
        buildEventContext 
        (interpreter : IInterpreter<'TMetadata>) 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>) 
        (testEventStore : TestEventStore<'TMetadata>) 
        (persistedEvent : PersistedEvent<'TMetadata>) =
            let handlerPrograms = 
                EventfulHandlers.getHandlerPrograms persistedEvent handlers
            handlerPrograms 
            |> Seq.fold (runHandlerForEvent persistedEvent buildEventContext interpreter.Run) testEventStore

    let runMultiCommandEventHandler persistedEvent buildEventContext handlers (interpreter : IInterpreter<'TMetadata>) (testEventStore : TestEventStore<'TMetadata>)  (commands : 'TEventContext -> Eventful.MultiCommand.MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseType,'TMetadata>>) =
        let runCommand (cmd : obj) (cmdCtx : 'TCommandContext) (eventStore : TestEventStore<'TMetadata>) : (TestEventStore<'TMetadata> * CommandResult<'TBaseType,'TMetadata>) =
           let program = EventfulHandlers.getCommandProgram cmdCtx cmd handlers
           interpreter.Run program eventStore
        use context = buildEventContext persistedEvent

        TestMultiCommandInterpreter.interpret (commands context) runCommand testEventStore

    let runMultiCommandEventHandlers 
        buildEventContext 
        (interpreter : IInterpreter<'TMetadata>)  
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>) 
        (persistedEvent : PersistedEvent<'TMetadata>) 
        (testEventStore : TestEventStore<'TMetadata>) =
            let handlerPrograms = 
                EventfulHandlers.getMultiCommandEventHandlers persistedEvent handlers
            handlerPrograms 
            |> Seq.fold (runMultiCommandEventHandler persistedEvent buildEventContext handlers interpreter) testEventStore

    let getCurrentState streamId testEventStore =

        testEventStore.AggregateStateSnapShots
        |> Map.tryFind streamId
        |> Option.getOrElse StateSnapshot.Empty
        
    let applyEventDataToSnapshot 
        streamId
        body
        eventNumber
        metadata
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>)
        (testEventStore : TestEventStore<'TMetadata>) =
        let aggregateType = handlers.GetAggregateType metadata
        match handlers.AggregateTypes |> Map.tryFind aggregateType with
        | Some aggregateConfig ->
            let initialState = 
                getCurrentState streamId testEventStore

            let state' = 
                initialState
                |> AggregateStateBuilder.applyToSnapshot aggregateConfig.StateBuilder.GetBlockBuilders () body eventNumber metadata 

            let snapshots' = testEventStore.AggregateStateSnapShots |> Map.add streamId state'

            let wakeupQueue' =
                FSharpx.Option.maybe {
                    let! EventfulWakeupHandler(wakeupFold, _) = aggregateConfig.Wakeup
                    let! newTime = wakeupFold.GetState state'.State

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
        eventNumber
        (persistedStreamEntry : PersistedStreamEntry<'TMetadata>) 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>)
        (testEventStore : TestEventStore<'TMetadata>) =
        match persistedStreamEntry with
        | PersistedStreamEvent { Body = body; Metadata = metadata; StreamId = streamId } ->
            applyEventDataToSnapshot streamId body eventNumber metadata handlers testEventStore
        | PersistedStreamLink  { LinkedBody = body; LinkedMetadata = metadata; LinkedStreamId = streamId } ->
            applyEventDataToSnapshot streamId body eventNumber metadata handlers testEventStore

    let runEvent buildEventContext (interpreter : IInterpreter<'TMetadata>) handlers testEventStore streamEntry =
        match streamEntry with
        | PersistedStreamEvent persistedEvent ->
            runEventHandlers 
                buildEventContext 
                interpreter 
                handlers 
                testEventStore 
                persistedEvent
            |> runMultiCommandEventHandlers
                buildEventContext 
                interpreter 
                handlers 
                persistedEvent
                
        | PersistedStreamLink _ -> 
           testEventStore

    /// run all event handlers for produced events
    /// that have not been run yet
    let rec processPendingEvents 
        buildEventContext
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>) 
        (testEventStore : TestEventStore<'TMetadata>) =
        match testEventStore.PendingEvents with
        | Queue.Nil -> testEventStore
        | Queue.Cons ((eventNumber, streamEntry), xs) ->
            let next = 
                runEvent buildEventContext interpreter handlers { testEventStore with PendingEvents = xs } streamEntry
                |> updateStateSnapShot eventNumber streamEntry handlers
            processPendingEvents buildEventContext interpreter handlers next

    let runCommand interpreter cmd handler testEventStore =
        let program = handler cmd 
        interpreter program testEventStore

    let runWakeup
        (wakeupTime : UtcDateTime)
        (streamId : string)
        (aggregateType : string)
        interpreter 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>) 
        (testEventStore :  TestEventStore<'TMetadata>) = 

        maybe {
            let! aggregate = handlers.AggregateTypes |> Map.tryFind aggregateType
            let! EventfulWakeupHandler(wakeupFold, wakeupHandler) = aggregate.Wakeup
            return
                interpreter (wakeupHandler streamId wakeupTime) testEventStore
                |> fst
        }
        |> FSharpx.Option.getOrElse testEventStore

        
    let rec runToEnd 
        onTimeChange
        buildEventContext
        (interpreter : IInterpreter<'TMetadata>)  
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent>) 
        (testEventStore :  TestEventStore<'TMetadata>)
        : TestEventStore<'TMetadata> =
        let rec loop testEventStore =
            maybe {
                let! (w,ws) =  testEventStore.WakeupQueue |> PriorityQueue.tryPop 
                onTimeChange w.Time

                return runWakeup
                         w.Time
                         w.Stream
                         w.Type
                         (interpreter.Run)
                         handlers
                         { testEventStore with WakeupQueue = ws }
                        |> processPendingEvents buildEventContext interpreter handlers
                        |> loop
            } 
            |> FSharpx.Option.getOrElse testEventStore

        loop testEventStore