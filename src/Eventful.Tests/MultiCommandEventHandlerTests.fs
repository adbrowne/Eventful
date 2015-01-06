namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing
open FSharpx

open Xunit
open FsUnit.Xunit
open FSharp.Control

module MultiCommandEventHandlerTests = 
    open EventSystemTestCommon

    type FooCmd = {
        Id : Guid
    }

    type FooEvent = {
        Id : Guid
    }
    with interface IEvent

    type UberEvent = {
        Id : Guid
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<BarEvent>
    }

    let fooHandlers () =    
        let cmdHandlers = seq {
            yield 
                (fun (cmd:FooCmd) -> 
                    {
                        FooEvent.Id = cmd.Id
                    })
                |> cmdHandler
                |> AggregateActionBuilder.buildCmd
        }

        let multiCmdHandler (e : BarEvent) (eventContext : UnitEventContext)  =
            asyncSeq {
                let cmd = { FooCmd.Id = e.Id } :> obj
                yield (cmd, Guid.NewGuid())
            }

        let handlers =
             AggregateHandlers.Empty
             |> AggregateHandlers.addCommandHandlers cmdHandlers
             |> AggregateHandlers.addMultiCommandEventHandler (multiCmdHandler |> AggregateActionBuilder.multiCommandEventHandler)

        Eventful.Aggregate.aggregateDefinitionFromHandlers 
            "TestAggregate" 
            TestMetadata.GetUniqueId
            getCommandStreamName 
            getStreamName 
            handlers

    let handlers : Eventful.EventfulHandlers<Guid,UnitEventContext,_,IEvent> =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst UnitEventContext) handlers

    let fooEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:FooEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``BarEvent produces FooCmd and then FooEvent`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName UnitEventContext thisId

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.injectEvent 
                "fake stream" 
                ({ BarEvent.Id = thisId } :> IEvent)
                { TestMetadata.AggregateType = "TestAggregate" 
                  SourceMessageId = None }

        let fooCount = afterRun.EvaluateState streamName thisId fooEventCounter

        fooCount |> should equal 1