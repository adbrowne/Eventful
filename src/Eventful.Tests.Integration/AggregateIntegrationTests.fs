namespace Eventful.Tests.Integration

open Xunit
open System
open FsUnit.Xunit
open FSharpx
open Eventful
open Eventful.EventStream
open Eventful.EventStore
open Eventful.Aggregate
open Eventful.AggregateActionBuilder
open Eventful.Testing

open FSharpx.Option

open TestEventStoreSystemHelpers

type AggregateIntegrationTests () = 

    let mutable system : EventStoreSystem<unit, MockDisposable, TestMetadata, obj> option = None

    let streamPositionMap : Map<string, int> ref = ref Map.empty

    let waitFor f : Async<unit> =
        let timeout = DateTime.UtcNow.AddSeconds(20.0).Ticks

        async {
            while (not (f()) && DateTime.UtcNow.Ticks < timeout) do
                do! Async.Sleep(100)
        }

    let eventCounterStateBuilder =
        StateBuilder.Empty "eventCount" 0
        |> StateBuilder.handler (fun (e : WidgetCreatedEvent) (m : TestMetadata) -> e.WidgetId)  (fun (s, (e : WidgetCreatedEvent),m) -> s + 1)
        |> (fun x -> x :> IStateBuilder<_, _, _>)

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Can run command`` () : unit =
        streamPositionMap := Map.empty
        let newEvent (position, streamId, eventNumber, a:EventStreamEventData<TestMetadata>) =
            IntegrationTests.log.Error <| lazy(sprintf "Received event %s" a.EventType)
            streamPositionMap := !streamPositionMap |> Map.add streamId eventNumber

        async {
            let system = system.Value
            system.AddOnCompleteEvent newEvent

            let widgetId = { WidgetId.Id = Guid.NewGuid() }

            let! cmdResult = 
                system.RunCommand
                    ()
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
            | Choice1Of2 ({Events = [(streamName, event, metadata)]}) ->
                streamName |> should equal expectedStreamName
                event |> should equal expectedEvent

                do! waitFor (fun () -> !streamPositionMap |> Map.tryFind expectedStreamName |> Option.getOrElse (-1) >= 0)
                let counterStream = sprintf "WidgetCounter-%s" (widgetId.Id.ToString("N"))

                let countsEventProgram = eventCounterStateBuilder |> AggregateStateBuilder.toStreamProgram counterStream widgetId
                let! (eventsConsumed, count) = system.RunStreamProgram countsEventProgram

                eventCounterStateBuilder.GetState count |> should equal 1
                return ()
            | x ->
                Assert.True(false, sprintf "Expected one success event instead of %A" x)

        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Can run many commands`` () : unit =
        streamPositionMap := Map.empty
        let newEvent (position, streamId, eventNumber, a:EventStreamEventData<TestMetadata>) =
            streamPositionMap := !streamPositionMap |> Map.add streamId eventNumber
            IntegrationTests.log.Error <| lazy(sprintf "Received event %s" a.EventType)

        async {
            let system = system.Value

            system.AddOnCompleteEvent newEvent

            let widgetId = { WidgetId.Id = Guid.NewGuid() }

            for _ in [1..10] do
                let! cmdResult = 
                    system.RunCommand
                        ()
                        { 
                            CreateWidgetCommand.WidgetId = widgetId; 
                            Name = "Mine"
                        }

                IntegrationTests.log.Debug <| lazy (sprintf "%A" cmdResult)
                ()

            let expectedStreamName = sprintf "Widget-%s" (widgetId.Id.ToString("N"))

            let expectedEvent = {
                WidgetCreatedEvent.WidgetId = widgetId
                Name = "Mine"
            }

            do! waitFor (fun () -> !streamPositionMap |> Map.tryFind expectedStreamName |> Option.getOrElse (-1) >= 9)
            let counterStream = sprintf "WidgetCounter-%s" (widgetId.Id.ToString("N"))

            let countsEventProgram = eventCounterStateBuilder |> AggregateStateBuilder.toStreamProgram counterStream widgetId
            let! (eventsConsumed, count) = system.RunStreamProgram countsEventProgram
            eventCounterStateBuilder.GetState count |> should equal 10

        } |> Async.RunSynchronously

    interface Xunit.IUseFixture<TestEventStoreSystemFixture> with
        member x.SetFixture(fixture) =
            system <- Some fixture.System