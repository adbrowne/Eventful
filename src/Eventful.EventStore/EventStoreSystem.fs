namespace Eventful.EventStore

open Eventful
open EventStore.ClientAPI

open System
open FSharpx
open FSharpx.Collections
open FSharpx.Functional

type EventStoreSystem<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent when 'TMetadata : equality and 'TEventContext :> System.IDisposable> 
    ( 
        handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent>,
        client : EventStoreClient,
        serializer: ISerializer,
        getEventContextFromMetadata : PersistedEvent<'TMetadata> -> 'TEventContext,
        getSnapshot,
        buildWakeupMonitor : (string -> string -> UtcDateTime -> unit) -> IWakeupMonitor
    ) =

    let log = createLogger "Eventful.EventStoreSystem"
    [<Literal>]
    let positionStream = "EventStoreProcessPosition"
    let mutable lastPositionUpdate = EventPosition.Start

    let mutable lastEventProcessed : EventPosition = EventPosition.Start
    let mutable onCompleteCallbacks : List<EventPosition * string * int * EventStreamEventData<'TMetadata> -> unit> = List.empty
    let mutable timer : System.Threading.Timer = null
    let mutable subscription : EventStoreAllCatchUpSubscription = null
    let completeTracker = new LastCompleteItemAgent<EventPosition>()

    let updatePositionLock = obj()
    let updatePosition _ = async {
        try
            return!
                lock updatePositionLock 
                    (fun () -> 
                        async {
                            let! lastComplete = completeTracker.LastComplete()
                            log.Debug <| lazy ( sprintf "Updating position %A" lastComplete )
                            match lastComplete with
                            | Some position when position <> lastPositionUpdate ->
                                do! ProcessingTracker.setPosition client positionStream position
                                lastPositionUpdate <- position
                            | _ -> () })
        with | e ->
            log.ErrorWithException <| lazy("failure updating position", e)}

    let inMemoryCache = new System.Runtime.Caching.MemoryCache("EventfulEvents")

    let logStart (correlationId : Guid option) name extraTemplate (extraVariables : obj[]) =
        let contextId = Guid.NewGuid()
        let startMessageTemplate = sprintf "Start %s: %s {@CorrelationId} {@ContextId}" name extraTemplate 
        let startMessageVariables = Array.append extraVariables [|correlationId;contextId|]
        log.RichDebug startMessageTemplate startMessageVariables
        let sw = startStopwatch()
        {
            ContextStartData.ContextId = contextId
            CorrelationId = correlationId
            Stopwatch = sw
            Name = name
            ExtraTemplate = extraTemplate
            ExtraVariables = extraVariables
        }

    let logEnd (startData : ContextStartData) = 
        let elapsed = startData.Stopwatch.ElapsedMilliseconds 
        let completeMessageTemplate = sprintf "Complete %s: {@CorrelationId} {@ContextId} {Elapsed:000} ms" startData.Name
        let completeMessageVariables : obj[] = [|startData.CorrelationId;startData.ContextId;elapsed|]
        log.RichDebug completeMessageTemplate completeMessageVariables

    let logException (startData : ContextStartData) (ex) = 
        let elapsed = startData.Stopwatch.ElapsedMilliseconds 
        let messageTemplate = sprintf "Exception in %s: {@Exception} %s {@CorrelationId} {@ContextId} {Elapsed:000} ms" startData.Name startData.ExtraTemplate
        let messageVariables : obj[] = 
            seq {
                yield (ex :> obj)
                yield! startData.ExtraVariables |> Seq.ofArray
                yield startData.CorrelationId :> obj
                yield startData.ContextId :> obj
                yield elapsed :> obj
            }
            |> Array.ofSeq
        log.RichError messageTemplate messageVariables
        
    let interpreter program = 
        EventStreamInterpreter.interpret 
            client 
            inMemoryCache 
            serializer 
            handlers.EventStoreTypeToClassMap 
            handlers.ClassToEventStoreTypeMap
            getSnapshot 
            program

    let runCommand context cmd = async {
        let correlationId = handlers.GetCommandCorrelationId context
        let startContext = logStart correlationId "command handler" "{@Command}" [|cmd|]
        let program = EventfulHandlers.getCommandProgram context cmd handlers
        let! result = 
            interpreter startContext program

        logEnd startContext
        return result
    }

    let runHandlerForEvent buildContext (persistedEvent : PersistedEvent<'TMetadata>) program =
        let correlationId = handlers.GetEventCorrelationId persistedEvent.Metadata
        async {
            let startContext = logStart correlationId "event handler" "{@EventType} {@StreamId} {@EventNumber}" [|persistedEvent.EventType;persistedEvent.StreamId;persistedEvent.EventNumber|]
            try
                use context = buildContext persistedEvent
                let! program = program context
                return! interpreter startContext program
            with | e ->
                logException startContext e
        }

    let runMultiCommandHandlerForEvent buildContext (persistedEvent : PersistedEvent<'TMetadata>) program =
        let correlationId = handlers.GetEventCorrelationId persistedEvent.Metadata
        async {
            let startContext = logStart correlationId "multi command event handler" "{@EventType} {@StreamId} {@EventNumber}" [|persistedEvent.EventType;persistedEvent.StreamId;persistedEvent.EventNumber|]
            try
                use context = buildContext persistedEvent
                do! MultiCommandInterpreter.interpret (program context) (flip runCommand)
            with | e ->
                logException startContext e
        }

    let runEventHandlers (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent>) (persistedEvent : PersistedEvent<'TMetadata>) =
        async {
            let regularEventHandlers = 
                handlers
                |> EventfulHandlers.getHandlerPrograms persistedEvent
                |> List.map (runHandlerForEvent getEventContextFromMetadata persistedEvent)

            let multiCommandEventHandlers =
                handlers
                |> EventfulHandlers.getMultiCommandEventHandlers persistedEvent
                |> List.map (runMultiCommandHandlerForEvent getEventContextFromMetadata persistedEvent)

            do! 
                List.append regularEventHandlers multiCommandEventHandlers
                |> Async.Parallel
                |> Async.Ignore
        }

    let runWakeupHandler streamId aggregateType time =
        let correlationId = Some <| Guid.NewGuid()
        let startContext = logStart correlationId "multi wakeup handler" "{@StreamId} {@AggregateType} {@Time}" [|streamId;aggregateType;time|]

        let config = handlers.AggregateTypes.TryFind aggregateType
        match config with
        | Some config -> 
            match config.Wakeup with
            | Some (EventfulWakeupHandler (_, handler)) ->
                handler streamId time
                |> interpreter startContext
                |> Async.RunSynchronously
            | None ->
                ()
        | None ->
            logException startContext (sprintf "Found wakeup for AggregateType %A but could not find any configuration" aggregateType)

    let wakeupMonitor = buildWakeupMonitor runWakeupHandler

    member x.PositionStream = positionStream
    member x.AddOnCompleteEvent callback = 
        onCompleteCallbacks <- callback::onCompleteCallbacks

    member x.RunStreamProgram correlationId name program = 
        async {
            let startContext = logStart correlationId name String.Empty Array.empty 
            let! result = interpreter startContext program
            logEnd startContext 
            return result
        }

    member x.Start () =  async {
        try
            do! ProcessingTracker.ensureTrackingStreamMetadata client positionStream
            let! currentEventStorePosition = ProcessingTracker.readPosition client positionStream
            let! nullablePosition = 
                match currentEventStorePosition with
                | x when x = EventPosition.Start ->
                    log.Debug <| lazy("No event position found. Starting from current head.")
                    async {
                        let! nextPosition = client.getNextPosition ()
                        return Nullable(nextPosition) }
                | _ -> 
                    async { return Nullable(currentEventStorePosition |> EventPosition.toEventStorePosition) }

            let timeBetweenPositionSaves = TimeSpan.FromSeconds(5.0)
            timer <- new System.Threading.Timer((updatePosition >> Async.RunSynchronously), null, TimeSpan.Zero, timeBetweenPositionSaves)
            subscription <- client.subscribe (nullablePosition |> Nullable.toOption) x.EventAppeared (fun () -> log.Debug <| lazy("Live"))
            wakeupMonitor.Start() 
        with | e ->
            log.ErrorWithException <| lazy("Exception starting EventStoreSystem",e)
            raise ( new System.Exception("See inner exception",e)) // cannot use reraise in an async block
        }

    member x.Stop () = 
        subscription.Stop()
        wakeupMonitor.Stop()
        if timer <> null then
            timer.Dispose()

    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
        match handlers.EventStoreTypeToClassMap.ContainsKey event.Event.EventType with
        | true ->
            let eventType = handlers.EventStoreTypeToClassMap.Item event.Event.EventType
            async {
                let position = { Commit = event.OriginalPosition.Value.CommitPosition; Prepare = event.OriginalPosition.Value.PreparePosition }
                completeTracker.Start position

                try
                    let evt = serializer.DeserializeObj (event.Event.Data) eventType

                    let metadata = (serializer.DeserializeObj (event.Event.Metadata) typeof<'TMetadata>) :?> 'TMetadata
                    let correlationId = handlers.GetEventCorrelationId metadata
                    log.RichDebug "Running Handlers for: {@EventType} {@StreamId} {@EventNumber} {@CorrelationId}" [|event.Event.EventType; event.OriginalEvent.EventStreamId; event.OriginalEvent.EventNumber;correlationId|]
                    let eventData = { Body = evt; EventType = event.Event.EventType; Metadata = metadata }

                    let eventStreamEvent = {
                        PersistedEvent.StreamId = event.Event.EventStreamId
                        EventNumber = event.Event.EventNumber
                        EventId = eventId
                        Body = evt
                        Metadata = metadata
                        EventType = event.Event.EventType
                    }

                    do! runEventHandlers handlers eventStreamEvent

                    for callback in onCompleteCallbacks do
                        callback (position, event.Event.EventStreamId, event.Event.EventNumber, eventData)
                with | e ->
                    log.RichError "Exception thrown while running event handlers {@Exception} {@StreamId} {@EventNumber}}" [|e;event.Event.EventStreamId; event.Event.EventNumber|]

                completeTracker.Complete position
            }
        | false -> 
            async {
                let position = event.OriginalPosition.Value |> EventPosition.ofEventStorePosition
                completeTracker.Start position
                completeTracker.Complete position
            }

    member x.RunCommand (context:'TCommandContext) (cmd : obj) = runCommand context cmd

    member x.LastEventProcessed = lastEventProcessed

    member x.EventStoreTypeToClassMap = handlers.EventStoreTypeToClassMap
    member x.ClassToEventStoreTypeMap = handlers.ClassToEventStoreTypeMap
    member x.Handlers = handlers

    interface IDisposable with
        member x.Dispose () = x.Stop()