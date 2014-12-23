namespace Eventful.EventStore

open Eventful
open EventStore.ClientAPI

open System
open FSharpx
open FSharpx.Collections

type EventStoreSystem<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType when 'TMetadata : equality and 'TEventContext :> System.IDisposable and 'TAggregateType : comparison> 
    ( 
        handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>,
        client : Client,
        serializer: ISerializer,
        getEventContextFromMetadata : PersistedEvent<'TMetadata> -> 'TEventContext,
        getSnapshot,
        buildWakeupMonitor : (string -> string -> DateTime -> unit) -> IWakeupMonitor
    ) =

    let log = createLogger "Eventful.EventStoreSystem"

    let mutable lastEventProcessed : EventPosition = EventPosition.Start
    let mutable onCompleteCallbacks : List<EventPosition * string * int * EventStreamEventData<'TMetadata> -> unit> = List.empty
    let mutable timer : System.Threading.Timer = null
    let mutable subscription : EventStoreAllCatchUpSubscription = null
    let completeTracker = new LastCompleteItemAgent<EventPosition>()

    let updatePosition _ = async {
        try
            let! lastComplete = completeTracker.LastComplete()
            log.Debug <| lazy ( sprintf "Updating position %A" lastComplete )
            match lastComplete with
            | Some position ->
                ProcessingTracker.setPosition client position |> Async.RunSynchronously
            | None -> () 
        with | e ->
            log.ErrorWithException <| lazy("failure updating position", e)}

    let inMemoryCache = new System.Runtime.Caching.MemoryCache("EventfulEvents")

    let interpreter program = 
        EventStreamInterpreter.interpret 
            client 
            inMemoryCache 
            serializer 
            handlers.EventStoreTypeToClassMap 
            handlers.ClassToEventStoreTypeMap
            getSnapshot 
            program

    let runHandlerForEvent (persistedEvent : PersistedEvent<'TMetadata>) program =
        let correlationId = Guid.NewGuid()
        async {
            try
                let! program = program
                return! interpreter correlationId program
            with | e ->
                log.ErrorWithException <| lazy(sprintf "Exception in event handler: Stream: %s EventNumber: %d" persistedEvent.StreamId persistedEvent.EventNumber, e)
        }

    let runEventHandlers (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>) (persistedEvent : PersistedEvent<'TMetadata>) =
        async {
            do! 
                handlers
                |> EventfulHandlers.getHandlerPrograms getEventContextFromMetadata persistedEvent
                |> List.map (runHandlerForEvent persistedEvent)
                |> Async.Parallel
                |> Async.Ignore
        }

    let runWakeupHandler streamId aggregateTypeString time =
        let correlationId = Guid.NewGuid()
        let aggregateType = handlers.StringToAggregateType aggregateTypeString
        log.RichDebug "RunWakeupHandler {@StreamId} {@AggregateType} {@Time} {@CorrelationId}" [|streamId;aggregateType;time;correlationId|]

        let config = handlers.AggregateTypes.Item aggregateType
        match config.Wakeup with
        | Some (EventfulWakeupHandler (_, handler)) ->
            handler streamId time
            |> interpreter correlationId
            |> Async.RunSynchronously
        | None ->
            ()

    let wakeupMonitor = buildWakeupMonitor runWakeupHandler

    member x.AddOnCompleteEvent callback = 
        onCompleteCallbacks <- callback::onCompleteCallbacks

    member x.RunStreamProgram program = interpreter (Guid.NewGuid()) program

    member x.Start () =  async {
        try
            let! position = ProcessingTracker.readPosition client |> Async.map (Option.map EventPosition.toEventStorePosition)
            let! nullablePosition = match position with
                                    | Some position -> async { return  Nullable(position) }
                                    | None -> 
                                        log.Debug <| lazy("No event position found. Starting from current head.")
                                        async {
                                            let! nextPosition = client.getNextPosition ()
                                            return Nullable(nextPosition) }

            let timeBetweenPositionSaves = TimeSpan.FromSeconds(5.0)
            timer <- new System.Threading.Timer((updatePosition >> Async.RunSynchronously), null, TimeSpan.Zero, timeBetweenPositionSaves)
            subscription <- client.subscribe position x.EventAppeared (fun () -> log.Debug <| lazy("Live"))
            wakeupMonitor.Start() 
        with | e ->
            log.ErrorWithException <| lazy("Exception starting EventStoreSystem",e)
            raise ( new System.Exception("See inner exception",e)) // cannot use reraise in an async block
        }

    member x.Stop () = 
        if timer <> null then
            timer.Dispose()

    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
        log.Debug <| lazy(sprintf "EventAppeared: %A: %A %A" event.Event.EventType event.OriginalEvent.EventStreamId event.OriginalEvent.EventNumber)
        match handlers.EventStoreTypeToClassMap.ContainsKey event.Event.EventType with
        | true ->
            let eventType = handlers.EventStoreTypeToClassMap.Item event.Event.EventType
            log.Debug <| lazy(sprintf "Running Handler for: %A: %A %A" event.Event.EventType event.OriginalEvent.EventStreamId event.OriginalEvent.EventNumber)
            async {
                let position = { Commit = event.OriginalPosition.Value.CommitPosition; Prepare = event.OriginalPosition.Value.PreparePosition }
                completeTracker.Start position
                let evt = serializer.DeserializeObj (event.Event.Data) eventType

                let metadata = (serializer.DeserializeObj (event.Event.Metadata) typeof<'TMetadata>) :?> 'TMetadata
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

                completeTracker.Complete position

                for callback in onCompleteCallbacks do
                    callback (position, event.Event.EventStreamId, event.Event.EventNumber, eventData)
            }
        | false -> 
            async {
                let position = event.OriginalPosition.Value |> EventPosition.ofEventStorePosition
                completeTracker.Start position
                completeTracker.Complete position
            }

    member x.RunCommand (context:'TCommandContext) (cmd : obj) =
        async {
            let correlationId = Guid.NewGuid()
            log.RichDebug "Running command: {@Command} {@CorrelationId}" [|cmd;correlationId|]
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let program = EventfulHandlers.getCommandProgram context cmd handlers
            let! result = 
                interpreter correlationId program

            sw.Stop()
            log.RichDebug "Command complete: {@Command} {@Result} {@CorrelationId} {Elapsed:000} ms" [|cmd;result;correlationId;sw.ElapsedMilliseconds|]
            return result
        }

    member x.LastEventProcessed = lastEventProcessed

    member x.EventStoreTypeToClassMap = handlers.EventStoreTypeToClassMap
    member x.ClassToEventStoreTypeMap = handlers.ClassToEventStoreTypeMap
    member x.Handlers = handlers

    interface IDisposable with
        member x.Dispose () = x.Stop()