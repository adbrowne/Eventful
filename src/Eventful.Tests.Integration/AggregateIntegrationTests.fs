namespace Eventful.Tests.Integration

open Xunit
open System
open EventStore.ClientAPI
open FsUnit.Xunit
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
    member x.RunCommand (cmd : obj) = 
        async {
            let program = EventfulHandlers.getCommandProgram cmd handlers
            let! result = EventStreamInterpreter.interpret client serializer program 
            return result
        }

module AggregateIntegrationTests = 
    type AggregateType =
    | Widget
    | WidgetCounter

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

    let newSystem client = new EventStoreSystem(handlers, client, RunningTests.esSerializer)

    [<Fact>]
    let ``Can run command`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let system = newSystem client

            let widgetId = { WidgetId.Id = Guid.NewGuid() }
            let! ignored = 
                system.RunCommand
                    { 
                        CreateWidgetCommand.WidgetId = widgetId; 
                        Name = "Mine"
                    }

            return ()
        } |> Async.RunSynchronously