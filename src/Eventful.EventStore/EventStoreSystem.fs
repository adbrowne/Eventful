namespace Eventful.EventStore

open Eventful
open EventStore.ClientAPI

open System
open FSharpx

type EventStoreSystem<'TCommandContext, 'TEventContext,'TMetadata when 'TMetadata : equality> 
    ( 
        handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata>,
        client : Client,
        serializer: ISerializer,
        eventContext : 'TEventContext
    ) =

    let toGesPosition position = new EventStore.ClientAPI.Position(position.Commit, position.Prepare)
    let toEventfulPosition (position : Position) = { Commit = position.CommitPosition; Prepare = position.PreparePosition }

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

    let interpreter program = EventStreamInterpreter.interpret client serializer handlers.EventTypeMap program

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
        let! position = ProcessingTracker.readPosition client |> Async.map (Option.map toGesPosition)
        let! nullablePosition = match position with
                                | Some position -> async { return  Nullable(position) }
                                | None -> 
                                    log.Debug <| lazy("No event position found. Starting from current head.")
                                    async {
                                        let! nextPosition = client.getNextPosition ()
                                        return Nullable(nextPosition) }

        let timeBetweenPositionSaves = TimeSpan.FromSeconds(5.0)
        timer <- new System.Threading.Timer((updatePosition >> Async.RunSynchronously), null, TimeSpan.Zero, timeBetweenPositionSaves)
        subscription <- client.subscribe position x.EventAppeared (fun () -> ()) }

    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
        log.Debug <| lazy(sprintf "Received: %A: %A %A" eventId event.Event.EventType event.OriginalPosition)

        match handlers.EventTypeMap|> Bimap.tryFind event.Event.EventType with
        | Some (eventType) ->
            async {
                let position = { Commit = event.OriginalPosition.Value.CommitPosition; Prepare = event.OriginalPosition.Value.PreparePosition }
                completeTracker.Start position
                let evt = serializer.DeserializeObj (event.Event.Data) eventType.RealType.AssemblyQualifiedName

                let metadata = (serializer.DeserializeObj (event.Event.Metadata) typeof<'TMetadata>.AssemblyQualifiedName) :?> 'TMetadata
                let eventData = { Body = evt; EventType = event.Event.EventType; Metadata = metadata }
                let eventStreamEvent = EventStreamEvent.Event eventData

                do! runEventHandlers handlers (event.Event.EventStreamId, event.Event.EventNumber, eventStreamEvent)

                completeTracker.Complete position

                for callback in onCompleteCallbacks do
                    callback (position, event.Event.EventStreamId, event.Event.EventNumber, eventData)
            }
        | None -> 
            async {
                let position = event.OriginalPosition.Value |> toEventfulPosition
                completeTracker.Start position
                completeTracker.Complete position
            }

    member x.RunCommand (context:'TCommandContext) (cmd : obj) = 
        async {
            let program = EventfulHandlers.getCommandProgram context cmd handlers
            let! result = EventStreamInterpreter.interpret client serializer handlers.EventTypeMap program
            return result
        }

    member x.EventTypeMap = handlers.EventTypeMap

    member x.LastEventProcessed = lastEventProcessed