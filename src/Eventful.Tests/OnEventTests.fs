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

        let evtHandlers : seq<IEventHandler<_,_,UnitEventContext, IEvent>> = seq {
            yield 
                AggregateActionBuilder.onEvent 
                    (fun (e : FooEvent) _ -> e.Id) 
                    StateBuilder.nullStateBuilder 
                    (fun s e c -> 
                        let uniqueId = sprintf "FooEvent:%s" (e.Id.ToString());
                        let metadata = metadataBuilder (Some uniqueId)
                        ({ BarEvent.Id = e.Id } :> IEvent, metadata)
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

    let handlers : Eventful.EventfulHandlers<unit,UnitEventContext,_,IEvent,_> =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst UnitEventContext) handlers

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``FooEvent produces BarEvent`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName UnitEventContext thisId

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.injectEvent 
                "fake stream" 
                ({ FooEvent.Id = thisId } :> IEvent)
                { TestMetadata.AggregateType = "TestAggregate" 
                  SourceMessageId = None }

        let barCount = afterRun.EvaluateState streamName thisId barEventCounter

        barCount |> should equal 1

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Same event will not be run twice`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName UnitEventContext thisId

        let event = { FooEvent.Id = thisId } :> IEvent
        let afterRun = 
            emptyTestSystem  
            |> TestSystem.injectEvent streamName event { TestMetadata.AggregateType = "Foo"; SourceMessageId = (Some "") } // first run
            |> TestSystem.injectEvent streamName event { TestMetadata.AggregateType = "Foo"; SourceMessageId = (Some "") } // second run
            |> TestSystem.runToEnd

        let barStateIs1 guid =
            afterRun.EvaluateState (getStreamName UnitEventContext guid) guid barEventCounter |> should equal 1

        barStateIs1 thisId

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
                    ({ BarEvent.Id = aggregateId } :> IEvent, metadataBuilder None)
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
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst UnitEventContext) handlers

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
            afterRun.EvaluateState (getStreamName UnitEventContext guid) guid barEventCounter |> should equal 1

        barStateIs1 thisId
        barStateIs1 secondId
