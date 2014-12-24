namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing
open FSharpx

open Xunit
open FsUnit.Xunit
open Swensen.Unquote

module WakeupTests =
    open EventSystemTestCommon

    let metadataBuilder sourceMessageId = { 
        SourceMessageId = sourceMessageId 
        AggregateType =  "TestAggregate" }

    type FooCmd = {
        Id : Guid
    }

    type WakeupRunEvent = {
        Id : Guid
        TimeRun : DateTime
    }
    with interface IEvent

    type FooEvent = {
        Id : Guid
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<WakeupRunEvent>
    }

    let fooHandlers wakeupTime =
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> 
                        { FooEvent.Id = cmd.Id } :> IEvent )
                |> AggregateActionBuilder.buildCmd
        }

        let wakeupCount =
            StateBuilder.eventTypeCountBuilder (fun (evt:WakeupRunEvent) _ -> ())

        let fooCount =
            StateBuilder.eventTypeCountBuilder (fun (evt:FooEvent) _ -> ())

        let wakeupBuilder wakeupTime =
            AggregateStateBuilder.tuple2 fooCount wakeupCount
            |> AggregateStateBuilder.map (function
                | (1, 0) ->
                    Some wakeupTime
                | (1, 1) ->
                    Some wakeupTime
                | _ -> None)

        let evtHandlers = Seq.empty

        let onWakeup (time : DateTime) () =
            Seq.singleton ({ WakeupRunEvent.Id = Guid.NewGuid(); TimeRun = time } :> IEvent, EventSystemTestCommon.metadataBuilder)

        Eventful.Aggregate.toAggregateDefinition 
            "TestAggregate"
            TestMetadata.GetUniqueId
            getCommandStreamName 
            getStreamName 
            cmdHandlers
            evtHandlers
        |> Eventful.Aggregate.withWakeup 
            (wakeupBuilder wakeupTime)
            StateBuilder.nullStateBuilder 
            onWakeup

    let handlers wakeupTime = 
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (fooHandlers wakeupTime)
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem wakeupTime = TestSystem.Empty (konst UnitEventContext) (handlers wakeupTime)

    let fooEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:FooEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    let wakeupTimeBuilder : IStateBuilder<DateTime option, TestMetadata, unit> =
        StateBuilder.Empty "RunTime" None
        |> StateBuilder.handler (fun _ _ -> ()) (fun (_,e:WakeupRunEvent,_) -> Some e.TimeRun)
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Wakeup event is run on time`` () : unit =
        let thisId = Guid.NewGuid() 
        let wakeupTime = DateTime.UtcNow.AddDays(1.0)
        let streamName = getStreamName UnitEventContext thisId

        let commandId = Guid.NewGuid() 

        let afterRun = 
            emptyTestSystem wakeupTime
            |> TestSystem.runCommand { FooCmd.Id = thisId } commandId
            |> TestSystem.runToEnd

        let actualTimeRun = afterRun.EvaluateState streamName () wakeupTimeBuilder

        actualTimeRun |> should equal (Some wakeupTime)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can chain wakeup events`` () : unit =
        let thisId = Guid.NewGuid()
        let wakeupTime = DateTime.UtcNow.AddDays(1.0)
        let streamName = getStreamName UnitEventContext thisId

        let commandId = Guid.NewGuid() 

        let afterRun = 
            emptyTestSystem wakeupTime
            |> TestSystem.runCommand { FooCmd.Id = thisId } commandId
            |> TestSystem.runToEnd

        let wakeupCount = afterRun.EvaluateState streamName () (StateBuilder.eventTypeCountBuilder (fun (e:WakeupRunEvent) _ -> ()))

        wakeupCount |> should equal 2

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Throws if wakeupTime is not UTC`` () : unit =
        // note this test only checks the test system
        // this test does not prove that there will be an exception thrown by EventStoreSystem
        let thisId = Guid.NewGuid()
        let wakeupTime = DateTime.Now.AddDays(1.0)
        let streamName = getStreamName UnitEventContext thisId

        let commandId = Guid.NewGuid() 

        Swensen.Unquote.Assertions.raisesWith 
            <@ emptyTestSystem wakeupTime
            |> TestSystem.runCommand { FooCmd.Id = thisId } commandId
            |> TestSystem.runToEnd @>
            (fun (e:exn) -> <@ e.Message = "WakeupTime must be in UTC" @>)