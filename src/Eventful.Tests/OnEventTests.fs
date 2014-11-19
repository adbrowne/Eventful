namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing
open FSharpx

open Xunit
open FsUnit.Xunit
open FSharp.Control

module OnEventTests =
    open EventSystemTestCommon

    type FooEvent = {
        Id : Guid
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<BarEvent>
    }

    let fooHandlers () =    
        let cmdHandlers = Seq.empty

        let evtHandlers : seq<IEventHandler<_,_,_>> = seq {
            yield 
                AggregateActionBuilder.onEvent 
                    (fun (e : FooEvent) _ -> e.Id) 
                    StateBuilder.nullStateBuilder 
                    (fun s e -> 
                        ({ BarEvent.Id = e.Id } :> obj, metadataBuilder)
                        |> Seq.singleton
                    )
        }

        Eventful.Aggregate.toAggregateDefinition 
            "TestAggregate" 
            TestMetadata.GetUniqueId
            getCommandStreamName 
            getStreamName 
            cmdHandlers 
            evtHandlers

    let handlers : Eventful.EventfulHandlers<unit,_,_,IEvent,_> =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst ()) handlers

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``FooEvent produces BarEvent`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName () thisId
        let commandUniqueId = Guid.NewGuid()

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.injectEvent 
                "fake stream" 
                0 
                ({ FooEvent.Id = thisId } :> IEvent)
                { 
                    TestMetadata.AggregateType = "TestAggregate" 
                    SourceMessageId = ""}

        let barCount = afterRun.EvaluateState streamName thisId barEventCounter

        barCount |> should equal 1

/// Test delivering an OnEvent to multiple
/// aggregate instances
module OnEventMultiAggregateTests =
    open EventSystemTestCommon

    type FooCmd = {
        Id : Guid
        SecondId : Guid
    }

    type FooEvent = {
        Id : Guid
        SecondId : Guid
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<BarEvent>
    }

    let fooHandlers () =    
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> 
                        {
                            FooEvent.Id = cmd.Id 
                            SecondId = cmd.SecondId
                        }
                    )    
                |> AggregateActionBuilder.withCmdId (fun cmd -> cmd.Id)
                |> AggregateActionBuilder.buildCmd
        }

        let evtHandlers = seq {
            let h = (fun aggregateId s -> 
                    ({ BarEvent.Id = aggregateId } :> obj, metadataBuilder)
                    |> Seq.singleton
                )
            yield 
                AggregateActionBuilder.onEventMulti
                    StateBuilder.nullStateBuilder 
                    (fun (e : FooEvent, _) -> seq {
                        yield (e.Id, h e.Id); 
                        yield (e.SecondId, h e.SecondId)
                    }) 
        }

        Eventful.Aggregate.toAggregateDefinition 
            "TestAggregate" 
            TestMetadata.GetUniqueId
            getCommandStreamName 
            getStreamName 
            cmdHandlers 
            evtHandlers

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst ()) handlers

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``FooEvent produces BarEvent`` () : unit =
        let thisId = Guid.NewGuid()
        let secondId = Guid.NewGuid()
        let commandUniqueId = Guid.NewGuid()

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId; SecondId = secondId } commandUniqueId

        let barStateIs1 guid =
            afterRun.EvaluateState (getStreamName () guid) guid barEventCounter |> should equal 1

        barStateIs1 thisId
        barStateIs1 secondId