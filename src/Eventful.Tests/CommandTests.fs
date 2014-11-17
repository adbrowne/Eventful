namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open Xunit
open FsUnit.Xunit

module CommandTests =
    open EventSystemTestCommon

    let metadataBuilder messageId sourceMessageId = { 
        MessageId = messageId 
        SourceMessageId = sourceMessageId
        AggregateType =  "TestAggregate" }

    let addEventType evtType handlers =
        handlers
        |> EventfulHandlers.addClassToEventStoreType evtType evtType.Name
        |> EventfulHandlers.addEventStoreType evtType.Name evtType 

    let addEventTypes evtTypes handlers =
        Seq.fold (fun h x -> addEventType x h) handlers evtTypes

    type FooCmd = {
        Id : Guid
    }

    type FooEvent = {
        Id : Guid
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
    }

    let fooHandlers : TestAggregateDefinition<_,_> =    
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> 
                        { FooEvent.Id = cmd.Id } :> IEvent )
                |> AggregateActionBuilder.buildCmd
        }

        let evtHandlers = Seq.empty

        Eventful.Aggregate.toAggregateDefinition 
            "TestAggregate" 
            TestMetadata.GetUniqueId
            getCommandStreamName 
            getStreamName 
            cmdHandlers 
            evtHandlers

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate fooHandlers
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty handlers

    let fooEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:FooEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Command with same unique id not run twice`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName () thisId

        // some unique id that can make the command processing idempotent
        let commandId = Guid.NewGuid() 

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId } commandId // first run
            |> TestSystem.runCommandNoThrow { FooCmd.Id = thisId } commandId // second run

        let fooCount = afterRun.EvaluateState streamName thisId fooEventCounter

        fooCount |> should equal 1

        match afterRun.LastResult with
        | Choice2Of2 msgs when msgs = FSharpx.Collections.NonEmptyList.singleton (CommandError "AlreadyProcessed") -> Assert.True(true)
        | _ -> Assert.True(false, "Command succeeded")