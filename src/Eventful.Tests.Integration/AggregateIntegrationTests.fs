namespace Eventful.Tests.Integration

open Xunit
open System
open EventStore.ClientAPI
open FsUnit.Xunit
open FSharpx
open Eventful
open Eventful.EventStream
open Eventful.EventStore
open Eventful.Aggregate
open Eventful.AggregateActionBuilder

open FSharpx.Option

type EventStoreSystem 
    ( 
        handlers : EventfulHandlers,
        client : Client,
        serializer: ISerializer
    ) =

    let toGesPosition position = new EventStore.ClientAPI.Position(position.Commit, position.Prepare)
    let toEventfulPosition (position : Position) = { Commit = position.CommitPosition; Prepare = position.PreparePosition }

    let log = createLogger "Eventful.EventStoreSystem"

    let mutable lastEventProcessed : EventPosition = EventPosition.Start
    let mutable onCompleteCallbacks : List<EventPosition * string * int * EventStreamEventData -> unit> = List.empty
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
            let program = evtHandler eventStream eventNumber evt
            return! interpreter program
        }

    let runEventHandlers (handlers : EventfulHandlers) (eventStream, eventNumber, eventStreamEvent) =
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
                do! completeTracker.Start position
                let evt = serializer.DeserializeObj (event.Event.Data) eventType.RealType.FullName

                let metadata = { MessageId = Guid.NewGuid(); SourceMessageId = Guid.NewGuid() } 
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
                do! completeTracker.Start position
                completeTracker.Complete position
            }

    member x.RunCommand (cmd : obj) = 
        async {
            let program = EventfulHandlers.getCommandProgram cmd handlers
            let! result = EventStreamInterpreter.interpret client serializer handlers.EventTypeMap program
            return result
        }

    member x.LastEventProcessed = lastEventProcessed

module AggregateIntegrationTests = 
    type AggregateType =
    | Widget
    | WidgetCounter
    with 
        interface IAggregateType 
            with member this.Name with get() = 
                                           match this with
                                           | Widget -> "Widget"
                                           | WidgetCounter -> "WidgetCounter"

    type WidgetId = 
        {
            Id : Guid
        } 
        interface IIdentity with
            member this.GetId = MagicMapper.getGuidId this

    type CreateWidgetCommand = {
        WidgetId : WidgetId
        Name : string
    }

    type WidgetCreatedEvent = {
        WidgetId : WidgetId
        Name : string
    }

    type WidgetEvents =
    | Created of WidgetCreatedEvent

    type WidgetCounterEvents =
    | Counted of WidgetCreatedEvent

    let stateBuilder = StateBuilder.Empty ()

    let widgetHandlers = 
        aggregate<unit,WidgetEvents,WidgetId,AggregateType> 
            AggregateType.Widget stateBuilder
            {
               let addWidget (cmd : CreateWidgetCommand) =
                   Created { 
                       WidgetId = cmd.WidgetId
                       Name = cmd.Name
               } 

               yield addWidget
                     |> simpleHandler stateBuilder
                     |> buildCmd
            }

    let widgetCounterAggregate =
        aggregate<unit,WidgetCounterEvents,WidgetId,AggregateType>
            AggregateType.WidgetCounter stateBuilder
            {
                let getId (evt : WidgetCreatedEvent) = evt.WidgetId
                yield linkEvent getId WidgetCounterEvents.Counted
            }

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate widgetHandlers
        |> EventfulHandlers.addAggregate widgetCounterAggregate

    let newSystem client = new EventStoreSystem(handlers, client, RunningTests.esSerializer)

    let streamPositionMap : Map<string, int> ref = ref Map.empty

    let waitFor f : Async<unit> =
        let timeout = DateTime.UtcNow.AddSeconds(20.0).Ticks

        async {
            while (not (f()) && DateTime.UtcNow.Ticks < timeout) do
                do! Async.Sleep(100)
        }

    let eventCounterStateBuilder =
        StateBuilder.Empty 0
        |> StateBuilder.addHandler (fun s (e : WidgetCreatedEvent) -> s + 1)

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Can run command`` () : unit =
        streamPositionMap := Map.empty
        let newEvent (position, streamId, eventNumber, a:EventStreamEventData) =
            streamPositionMap := !streamPositionMap |> Map.add streamId eventNumber
            IntegrationTests.log.Error <| lazy(sprintf "Received event %s" a.EventType)

        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let system = newSystem client

            system.AddOnCompleteEvent newEvent

            do! system.Start()

            let widgetId = { WidgetId.Id = Guid.NewGuid() }
            let! cmdResult = 
                system.RunCommand
                    { 
                        CreateWidgetCommand.WidgetId = widgetId; 
                        Name = "Mine"
                    }

            let expectedStreamName = sprintf "Widget-%s" (widgetId.Id.ToString("N"))

            let expectedEvent = {
                WidgetCreatedEvent.WidgetId = widgetId
                Name = "Mine"
            }

            match cmdResult with
            | Choice1Of2 ([(streamName, event, metadata)]) ->
                streamName |> should equal expectedStreamName
                event |> should equal expectedEvent
                do! waitFor (fun () -> !streamPositionMap |> Map.tryFind expectedStreamName |> Option.getOrElse (-1) >= 0)
                let counterStream = sprintf "WidgetCounter-%s" (widgetId.Id.ToString("N"))

                let countsEventProgram = eventCounterStateBuilder |> StateBuilder.toStreamProgram counterStream
                let! (eventsConsumed, count) = system.RunStreamProgram countsEventProgram

                count |> should equal (Some 1)
                return ()
            | x ->
                Assert.True(false, sprintf "Expected one success event instead of %A" x)

        } |> Async.RunSynchronously