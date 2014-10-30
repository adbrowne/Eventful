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

type WidgetId = {
    Id : Guid
} 

module TestEventStoreSystemHelpers =
    let emptyMetadata : Eventful.Testing.TestMetadata = { SourceMessageId = String.Empty; MessageId = Guid.Empty; AggregateId = Guid.Empty  }

    let systemConfiguration = {
        SystemConfiguration.GetUniqueId = (fun (x : TestMetadata) -> Some x.SourceMessageId)
        GetAggregateId = (fun (x : TestMetadata) -> { WidgetId.Id = x.AggregateId })
    }

    let inline buildMetadata aggregateId messageId sourceMessageId = { 
            SourceMessageId = sourceMessageId 
            MessageId = messageId 
            AggregateId = aggregateId }

    let inline withMetadata f cmd = 
        let cmdResult = f cmd
        (cmdResult, buildMetadata)

    let cmdBuilderS stateBuilder f =
        AggregateActionBuilder.fullHandler
            systemConfiguration 
            stateBuilder
            (fun state () cmd -> 
                let events = 
                    f state cmd 
                    |> (fun evt -> (evt :> obj, buildMetadata))
                    |> Seq.singleton

                let uniqueId = Guid.NewGuid().ToString()

                {
                    UniqueId = uniqueId
                    Events = events
                }
                |> Choice1Of2
            )

    let cmdHandler f =
        cmdBuilderS StateBuilder.nullStateBuilder (fun _ -> f)

    let inline onEvent fId s f = 
        let withMetadata s f = (f s) >> Seq.map (fun x -> (x, buildMetadata))
        Eventful.AggregateActionBuilder.onEvent fId s (withMetadata f)
    let inline linkEvent fId = 
        Eventful.AggregateActionBuilder.linkEvent fId buildMetadata

type AggregateType =
| Widget
| WidgetCounter

type CreateWidgetCommand = {
    WidgetId : WidgetId
    Name : string
}

type WidgetCreatedEvent = {
    WidgetId : WidgetId
    Name : string
}

open TestEventStoreSystemHelpers

// event store system running test system
type TestEventStoreSystemFixture () =
    let eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore ()

    let getStreamName typeName () (id:WidgetId) =
        sprintf "%s-%s" typeName (id.Id.ToString("N"))
        
    let widgetCmdHandlers = 
        seq {
               let addWidget (cmd : CreateWidgetCommand) =
                   { 
                       WidgetId = cmd.WidgetId
                       Name = cmd.Name } 

               yield addWidget
                     |> cmdHandler
                     |> buildCmd
            }

    let widgetHandlers = toAggregateDefinition (getStreamName "Widget") (getStreamName "Widget") (fun (x : WidgetId) -> x.Id) widgetCmdHandlers Seq.empty

    let widgetCounterEventHandlers =
        seq {
                let getId (evt : WidgetCreatedEvent) = evt.WidgetId
                yield linkEvent getId
            }

    let widgetCounterAggregate = toAggregateDefinition (getStreamName "WidgetCounter") (getStreamName "WidgetCounter") (fun (x : WidgetId) -> x.Id) Seq.empty widgetCounterEventHandlers

    let addEventType evtType handlers =
        handlers
        |> EventfulHandlers.addClassToEventStoreType evtType evtType.Name
        |> EventfulHandlers.addEventStoreType evtType.Name evtType 

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate widgetHandlers
        |> EventfulHandlers.addAggregate widgetCounterAggregate
        |> addEventType typeof<WidgetCreatedEvent>

    let client = new Client(eventStoreProcess.Connection)
    let newSystem client = new EventStoreSystem<unit,unit,Eventful.Testing.TestMetadata>(handlers, client, RunningTests.esSerializer, (fun _ -> ()))

    let system = newSystem client

    let mutable started = false
    do while not started do
        try
            do system.Start() |> Async.RunSynchronously
            started <- true
        with | _ -> started <- false

    member x.Connection = eventStoreProcess.Connection
    member x.System = system

    interface IDisposable with
        member this.Dispose () =
            (system :> IDisposable).Dispose()
            (eventStoreProcess :> IDisposable).Dispose()