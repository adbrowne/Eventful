namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open Xunit
open FsUnit.Xunit

module OnEventTests =
    open EventSystemTestCommon

    type FooCmd = {
        Id : Guid
    }

    type FooEvent = {
        Id : Guid
    }

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<BarEvent>
    }

    let fooHandlers () =    
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> { FooEvent.Id = cmd.Id })
                |> AggregateActionBuilder.buildCmd
        }

        let evtHandlers = seq {
            yield 
                AggregateActionBuilder.onEvent 
                    (fun (e : FooEvent) -> e.Id) 
                    StateBuilder.nullStateBuilder 
                    (fun s e -> 
                        ({ BarEvent.Id = e.Id }, metadataBuilder)
                        |> Seq.singleton
                    )
        }

        Eventful.Aggregate.toAggregateDefinition getCommandStreamName getStreamName id cmdHandlers evtHandlers

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty handlers

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``FooEvent produces BarEvent`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName () thisId
        let commandUniqueId = Guid.NewGuid()

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId } commandUniqueId

        let barCount = afterRun.EvaluateState streamName thisId barEventCounter

        barCount |> should equal 1

/// Test delivering an OnEvent to multiple
/// aggregate instances
module OnEventMuliAggregateTests =
    open EventSystemTestCommon

    type FooCmd = {
        Id : Guid
        SecondId : Guid
    }

    type FooEvent = {
        Id : Guid
        SecondId : Guid
    }

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
            yield 
                AggregateActionBuilder.onEventMulti 
                    (fun (e : FooEvent) -> seq { yield e.Id; yield e.SecondId }) 
                    StateBuilder.nullStateBuilder 
                    (fun aggregateId s e -> 
                        ({ BarEvent.Id = aggregateId }, metadataBuilder)
                        |> Seq.singleton
                    )
        }

        Eventful.Aggregate.toAggregateDefinition getCommandStreamName getStreamName id cmdHandlers evtHandlers

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty handlers

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