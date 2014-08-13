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
open Eventful.Testing

open FSharpx.Option

module IntegrationHelpers =
    let systemConfiguration = { 
        SetSourceMessageId = (fun id metadata -> { metadata with SourceMessageId = id })
        SetMessageId = (fun id metadata -> { metadata with MessageId = id })
    }

    let emptyMetadata : Eventful.Testing.TestMetadata = { SourceMessageId = String.Empty; MessageId = Guid.Empty }

    let inline simpleHandler s f = 
        let withMetadata f = f >> (fun x -> (x, emptyMetadata))
        Eventful.AggregateActionBuilder.simpleHandler systemConfiguration s (withMetadata f)
    let inline buildSimpleCmdHandler s f = 
        let withMetadata f = f >> (fun x -> (x, emptyMetadata))
        Eventful.AggregateActionBuilder.buildSimpleCmdHandler systemConfiguration s (withMetadata f)
    let inline onEvent fId s f = 
        let withMetadata f = f >> Seq.map (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty }))
        Eventful.AggregateActionBuilder.onEvent systemConfiguration fId s (withMetadata f)
    let inline linkEvent fId f = 
        let withMetadata f = f >> (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty }))
        Eventful.AggregateActionBuilder.linkEvent systemConfiguration fId f emptyMetadata

open IntegrationHelpers

module AggregateIntegrationTests = 
    type AggregateType =
    | Widget
    | WidgetCounter

    type WidgetId = 
        {
            Id : Guid
        } 

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

    let getStreamName typeName () (id:WidgetId) =
        sprintf "%s-%s" typeName (id.Id.ToString("N"))
        
    let widgetCmdHandlers = 
        seq {
               let addWidget (cmd : CreateWidgetCommand) =
                   Created { 
                       WidgetId = cmd.WidgetId
                       Name = cmd.Name
               } 

               yield addWidget
                     |> simpleHandler NamedStateBuilder.nullStateBuilder
                     |> buildCmd
            }

    let widgetHandlers = toAggregateDefinition (getStreamName "Widget") (getStreamName "Widget") widgetCmdHandlers Seq.empty

    let widgetCounterEventHandlers =
        seq {
                let getId (evt : WidgetCreatedEvent) = evt.WidgetId
                yield linkEvent getId WidgetCounterEvents.Counted
            }

    let widgetCounterAggregate = toAggregateDefinition (getStreamName "WidgetCounter") (getStreamName "WidgetCounter") Seq.empty widgetCounterEventHandlers

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate widgetHandlers
        |> EventfulHandlers.addAggregate widgetCounterAggregate

    let newSystem client = new EventStoreSystem<unit,unit,Eventful.Testing.TestMetadata>(handlers, client, RunningTests.esSerializer, ())

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
        let newEvent (position, streamId, eventNumber, a:EventStreamEventData<TestMetadata>) =
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

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Can run many command`` () : unit =
        streamPositionMap := Map.empty
        let newEvent (position, streamId, eventNumber, a:EventStreamEventData<TestMetadata>) =
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

            for _ in [1..1000] do
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

            do! waitFor (fun () -> !streamPositionMap |> Map.tryFind expectedStreamName |> Option.getOrElse (-1) >= 999)
            let counterStream = sprintf "WidgetCounter-%s" (widgetId.Id.ToString("N"))

            let countsEventProgram = eventCounterStateBuilder |> StateBuilder.toStreamProgram counterStream
            let! (eventsConsumed, count) = system.RunStreamProgram countsEventProgram

            count |> should equal (Some 1)

        } |> Async.RunSynchronously