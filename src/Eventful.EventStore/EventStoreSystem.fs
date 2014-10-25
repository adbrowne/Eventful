namespace Eventful.EventStore

open Eventful
open EventStore.ClientAPI

open System
open FSharpx
open FSharpx.Collections

type EventStoreSystem<'TCommandContext, 'TEventContext,'TMetadata when 'TMetadata : equality> 
    ( 
        handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata>,
        client : Client,
        serializer: ISerializer,
        eventContext : 'TEventContext
    ) =

    let log = createLogger "Eventful.EventStoreSystem"

    let mutable lastEventProcessed : EventPosition = EventPosition.Start
    let mutable onCompleteCallbacks : List<EventPosition * string * int * EventStreamEventData<'TMetadata> -> unit> = List.empty
    let mutable timer : System.Threading.Timer = null
    let mutable subscription : EventStoreAllCatchUpSubscription = null
    let completeTracker = new LastCompleteItemAgent<EventPosition>()

    let updatePosition _ = async {
        let! lastComplete = completeTracker.LastComplete()
        log.Debug <| lazy ( sprintf "Updating position %A" lastComplete )
        match lastComplete with
        | Some position ->
            ProcessingTracker.setPosition client position |> Async.RunSynchronously
        | None -> () }

    let inMemoryCache = new System.Runtime.Caching.MemoryCache("EventfulEvents")

    let interpreter program = EventStreamInterpreter.interpret client inMemoryCache serializer handlers.EventStoreTypeToClassMap handlers.ClassToEventStoreTypeMap program

    let runHandlerForEvent (eventStream, eventNumber, evt) (EventfulEventHandler (t, evtHandler)) =
        async {
            let program = evtHandler eventContext eventStream eventNumber evt
            return! interpreter program
        }

    let runEventHandlers (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata>) (eventStream, eventNumber, eventStreamEvent) =
        match eventStreamEvent with
        | EventStreamEvent.Event { Body = evt; EventType = eventType; Metadata = metadata } ->
            async {
                do! 
                    handlers.EventHandlers
                    |> Map.tryFind (evt.GetType().Name)
                    |> Option.getOrElse []
                    |> Seq.map (fun h -> runHandlerForEvent (eventStream, eventNumber, { Body = evt; EventType = eventType; Metadata = metadata }) h)
                    |> Async.Parallel
                    |> Async.Ignore
            }
        | _ ->
            async { () }

    member x.AddOnCompleteEvent callback = 
        onCompleteCallbacks <- callback::onCompleteCallbacks

    member x.RunStreamProgram program = interpreter program

    member x.Start () =  async {
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
        subscription <- client.subscribe position x.EventAppeared (fun () -> log.Debug <| lazy("Live")) }

    member x.Stop () = 
        if timer <> null then
            timer.Dispose()

    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
        match handlers.EventStoreTypeToClassMap.ContainsKey event.Event.EventType with
        | true ->
            let eventType = handlers.EventStoreTypeToClassMap.Item event.Event.EventType
            log.Debug <| lazy(sprintf "Running Handler for: %A: %A %A" event.Event.EventType event.OriginalEvent.EventStreamId event.OriginalEvent.EventNumber)
            async {
                let position = { Commit = event.OriginalPosition.Value.CommitPosition; Prepare = event.OriginalPosition.Value.PreparePosition }
                completeTracker.Start position
                let evt = serializer.DeserializeObj (event.Event.Data) eventType.AssemblyQualifiedName

                let metadata = (serializer.DeserializeObj (event.Event.Metadata) typeof<'TMetadata>.AssemblyQualifiedName) :?> 'TMetadata
                let eventData = { Body = evt; EventType = event.Event.EventType; Metadata = metadata }
                let eventStreamEvent = EventStreamEvent.Event eventData

                do! runEventHandlers handlers (event.Event.EventStreamId, event.Event.EventNumber, eventStreamEvent)

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
            let program = EventfulHandlers.getCommandProgram context cmd handlers
            let! result = 
                EventStreamInterpreter.interpret 
                    client 
                    inMemoryCache 
                    serializer 
                    handlers.EventStoreTypeToClassMap 
                    handlers.ClassToEventStoreTypeMap 
                    program
            return result
        }

    member x.LastEventProcessed = lastEventProcessed

    member x.EventStoreTypeToClassMap = handlers.EventStoreTypeToClassMap
    member x.ClassToEventStoreTypeMap = handlers.ClassToEventStoreTypeMap


    interface IDisposable with
        member x.Dispose () = x.Stop()
