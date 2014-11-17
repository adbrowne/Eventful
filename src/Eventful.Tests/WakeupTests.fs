namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open Xunit
open FsUnit.Xunit

module WakeupTests =
    open EventSystemTestCommon

    let metadataBuilder aggregateId messageId sourceMessageId = { 
        TestMetadata.AggregateId = aggregateId
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

    type WakeupRunEvent = {
        Id : Guid
    }
    with interface IEvent

    type FooEvent = {
        Id : Guid
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
    }

    let fooHandlers =    
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> 
                        { FooEvent.Id = cmd.Id } :> IEvent )
                |> AggregateActionBuilder.buildCmd
        }

        let wakeupBuilder = 
            StateBuilder.Empty "WakeupTime" None
            |> StateBuilder.aggregateStateHandler
                (fun (s, (e:FooEvent), m) -> Some DateTime.UtcNow)
            |> StateBuilder.aggregateStateHandler 
                (fun (s, (e:WakeupRunEvent), m) -> None)
            |> StateBuilder.toInterface

        let evtHandlers = Seq.empty

        let onWakeup (time : DateTime) () =
            Seq.singleton ({ WakeupRunEvent.Id = Guid.NewGuid() } :> IEvent, EventSystemTestCommon.metadataBuilder)

        Eventful.Aggregate.toAggregateDefinition 
            "TestAggregate"
            TestMetadata.GetUniqueId
            TestMetadata.GetAggregateId
            getCommandStreamName 
            getStreamName 
            cmdHandlers
            evtHandlers
        |> Eventful.Aggregate.withWakeup 
            wakeupBuilder 
            StateBuilder.nullStateBuilder 
            onWakeup

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate fooHandlers
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty handlers

    let fooEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:FooEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    let wakeupRunEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:WakeupRunEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Wakeup event is run one time`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName () thisId

        let commandId = Guid.NewGuid() 

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId } commandId
            |> TestSystem.runToEnd

        let wakeupRunCount = afterRun.EvaluateState streamName thisId wakeupRunEventCounter

        wakeupRunCount |> should equal 1