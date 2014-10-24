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

module TestEventStoreSystemHelpers =
    let systemConfiguration = { 
        SetSourceMessageId = (fun id metadata -> { metadata with SourceMessageId = id })
        SetMessageId = (fun id metadata -> { metadata with MessageId = id })
    }

    let emptyMetadata : Eventful.Testing.TestMetadata = { SourceMessageId = String.Empty; MessageId = Guid.Empty; AggregateId = Guid.Empty  }

    let inline buildMetadata aggregateId messageId sourceMessageId = { 
            SourceMessageId = sourceMessageId 
            MessageId = messageId 
            AggregateId = aggregateId }

    let inline withMetadata f cmd = 
        let cmdResult = f cmd
        (cmdResult, buildMetadata)

    let inline simpleHandler s f = 
        Eventful.AggregateActionBuilder.simpleHandler systemConfiguration s (withMetadata f)
    let inline buildSimpleCmdHandler s f = 
        Eventful.AggregateActionBuilder.buildSimpleCmdHandler systemConfiguration s (withMetadata f)
    let inline onEvent fId s f = 
        let withMetadata s f = (f s) >> Seq.map (fun x -> (x, buildMetadata))
        Eventful.AggregateActionBuilder.onEvent systemConfiguration fId s (withMetadata f)
    let inline linkEvent fId f = 
        let withMetadata f = f >> (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty; AggregateId = Guid.Empty }))
        Eventful.AggregateActionBuilder.linkEvent systemConfiguration fId f buildMetadata

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

open TestEventStoreSystemHelpers

// event store system running test system
type TestEventStoreSystemFixture () =
    let eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore ()

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
                     |> simpleHandler StateBuilder.nullStateBuilder
                     |> buildCmd
            }

    let widgetHandlers = toAggregateDefinition (getStreamName "Widget") (getStreamName "Widget") (fun (x : WidgetId) -> x.Id) widgetCmdHandlers Seq.empty

    let widgetCounterEventHandlers =
        seq {
                let getId (evt : WidgetCreatedEvent) = evt.WidgetId
                yield linkEvent getId WidgetCounterEvents.Counted
            }

    let widgetCounterAggregate = toAggregateDefinition (getStreamName "WidgetCounter") (getStreamName "WidgetCounter") (fun (x : WidgetId) -> x.Id) Seq.empty widgetCounterEventHandlers

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate widgetHandlers
        |> EventfulHandlers.addAggregate widgetCounterAggregate

    let client = new Client(eventStoreProcess.Connection)
    let newSystem client = new EventStoreSystem<unit,unit,Eventful.Testing.TestMetadata>(handlers, client, RunningTests.esSerializer, ())

    let system = newSystem client
    do system.Start() |> Async.RunSynchronously

    member x.Connection = eventStoreProcess.Connection
    member x.System = system

    interface IDisposable with
        member this.Dispose () =
            (eventStoreProcess :> IDisposable).Dispose()

