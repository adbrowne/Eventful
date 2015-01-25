namespace Eventful.Tests.Integration

open System
open FSharpx
open Eventful
open Eventful.EventStore
open Eventful.Aggregate
open Eventful.AggregateActionBuilder
open Eventful.Tests

type WidgetId = {
    Id : Guid
} 

module TestEventStoreSystemHelpers =
    let emptyMetadata : Eventful.Tests.Integration.TestMetadata = { 
        SourceMessageId = None
        AggregateType = "AggregateType" }

    let inline buildMetadata sourceMessageId = { 
            TestMetadata.SourceMessageId = sourceMessageId 
            AggregateType = "AggregateType" }

    let inline withMetadata f cmd = 
        let cmdResult = f cmd
        (cmdResult, buildMetadata)

    let cmdBuilderS stateBuilder f =
        AggregateActionBuilder.fullHandler
            MagicMapper.magicGetCmdId<_>
            stateBuilder
            (fun state () cmd -> 
                f state cmd 
                |> (fun evt -> (evt :> obj, buildMetadata None))
                |> Seq.singleton
                |> Choice1Of2
            )

    let cmdHandler f =
        cmdBuilderS StateBuilder.nullStateBuilder (fun _ -> f)

    let inline onEvent fId s f = 
        let runEvent eventState evt ctx = 
            f eventState evt ctx
            |> Seq.map (fun x -> (x, buildMetadata None))
            
        Eventful.AggregateActionBuilder.onEvent fId s runEvent
    let inline linkEvent fId = 
        Eventful.AggregateActionBuilder.linkEvent fId (Some >> buildMetadata)

type CreateWidgetCommand = {
    WidgetId : WidgetId
    Name : string
}

type WidgetCreatedEvent = {
    WidgetId : WidgetId
    Name : string
}

type MultiCommandCommand = {
    WidgetId : WidgetId
    Name : string
}

type MultiCommandEvent = {
    WidgetId : WidgetId
    Name : string
}

open TestEventStoreSystemHelpers

type MockDisposable = {
    mutable Disposed : bool
}
with 
    interface IDisposable with
        member x.Dispose() =
            x.Disposed <- true

// event store system running test system
type TestEventStoreSystemFixture () =
    let eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore ()

    let getStreamName typeName () (id:WidgetId) =
        sprintf "%s-%s" typeName (id.Id.ToString("N"))

    let getEventStreamName typeName (context : MockDisposable) (id:WidgetId) =
        sprintf "%s-%s" typeName (id.Id.ToString("N"))
        
    let widgetCmdHandlers = 
        seq {
               let addWidget (cmd : CreateWidgetCommand) =
                   { 
                       WidgetCreatedEvent.WidgetId = cmd.WidgetId
                       Name = cmd.Name } 

               yield addWidget
                     |> cmdHandler
                     |> buildCmd
            }

    let widgetHandlers = 
        toAggregateDefinition 
            "Widget" 
            TestMetadata.GetUniqueId
            (getStreamName "Widget") 
            (getEventStreamName "Widget") 
            widgetCmdHandlers 
            Seq.empty

    let widgetCounterEventHandlers =
        seq {
                let getId (evt : WidgetCreatedEvent) = evt.WidgetId
                yield linkEvent getId
            }

    let widgetCounterAggregate = 
        toAggregateDefinition 
            "WidgetCounter" 
            TestMetadata.GetUniqueId
            (getStreamName "WidgetCounter") 
            (getEventStreamName "WidgetCounter") 
            Seq.empty 
            widgetCounterEventHandlers

    let multiCommandAggregateCmdHandlers = seq {
        let handler (cmd : MultiCommandCommand) =
                       { 
                           MultiCommandEvent.WidgetId = cmd.WidgetId
                           Name = cmd.Name } 

        yield handler
             |> cmdHandler
             |> buildCmd 
    }

    let bigMultiHandler (evt : MultiCommandEvent) eventContext =
        Eventful.MultiCommand.multiCommand {
            let cmd = { CreateWidgetCommand.WidgetId = evt.WidgetId; Name = evt.Name} :> obj
            let! result = Eventful.MultiCommand.runCommand cmd ()
            ()
        }

    let multiCommandAggregateHandlers = 
        AggregateHandlers.Empty
        |> AggregateHandlers.addCommandHandlers multiCommandAggregateCmdHandlers
        |> AggregateHandlers.addMultiCommandEventHandler (AggregateActionBuilder.multiCommandEventHandler bigMultiHandler)

    let multiCommandAggregate =
        aggregateDefinitionFromHandlers
            "MultiCommand"
            TestMetadata.GetUniqueId
            (getStreamName "MultiCommand")
            (getEventStreamName "MultiCommand")
            multiCommandAggregateHandlers

    let aggregateThatThrowsEventHandlers =
        seq {
                let nullStateBuilder = StateBuilder.nullStateBuilder |> StateBuilder.toInterface
                let getId (evt : WidgetCreatedEvent) _ = evt.WidgetId
                let handler () evt ctx =
                    failwith "Some random exception"
                yield onEvent getId nullStateBuilder handler
            }

    let aggregateThatThrows =
        toAggregateDefinition
            "AggregateThatThrows"
            TestMetadata.GetUniqueId
            (getStreamName "AggregateThatThrows") 
            (getEventStreamName "AggregateThatThrows") 
            Seq.empty 
            aggregateThatThrowsEventHandlers

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate widgetHandlers
        |> EventfulHandlers.addAggregate widgetCounterAggregate
        |> EventfulHandlers.addAggregate aggregateThatThrows
        |> EventfulHandlers.addAggregate multiCommandAggregate
        |> StandardConventions.addEventType typeof<WidgetCreatedEvent>
        |> StandardConventions.addEventType typeof<MultiCommandEvent>

    let eventContexts = new System.Collections.Concurrent.ConcurrentQueue<MockDisposable>()

    let buildContext _ =
        let disposable = { MockDisposable.Disposed = false }
        eventContexts.Enqueue disposable
        disposable

    let nullGetSnapshot streamId typeMap = StateSnapshot.Empty |> Async.returnM

    let client = new EventStoreClient(eventStoreProcess.Connection)
    let mockWakeupMonitor _ = {
        new Eventful.IWakeupMonitor with
            member x.Start () = ()
            member x.Stop () = ()
    }
    let newSystem client = new EventStoreSystem<unit,MockDisposable,TestMetadata,obj>(handlers, client, RunningTests.esSerializer, buildContext, nullGetSnapshot, mockWakeupMonitor)

    let system = newSystem client

    let mutable started = false
    do while not started do
        try
            do system.Start() |> Async.RunSynchronously
            started <- true
        with | _ -> started <- false

    member x.Connection = eventStoreProcess.Connection
    member x.System = system
    member x.EventStoreAccess = eventStoreProcess

    interface IDisposable with
        member this.Dispose () =

            (system :> IDisposable).Dispose()
            (eventStoreProcess :> IDisposable).Dispose()

            let allEventContextsDisposed = 
                eventContexts
                |> Seq.forall (fun x -> x.Disposed)

            if not allEventContextsDisposed then
                failwith "Some eventcontexts were not disposed"