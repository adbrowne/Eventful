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