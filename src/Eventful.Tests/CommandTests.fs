namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open Xunit
open FsUnit.Xunit

module CommandTests =
    let metadataBuilder aggregateId messageId sourceMessageId = { 
        TestMetadata.AggregateId = aggregateId
        MessageId = messageId 
        SourceMessageId = sourceMessageId }

    let addEventType evtType handlers =
        handlers
        |> EventfulHandlers.addClassToEventStoreType evtType evtType.Name
        |> EventfulHandlers.addEventStoreType evtType.Name evtType 

    let addEventTypes evtTypes handlers =
        Seq.fold (fun h x -> addEventType x h) handlers evtTypes

    let getStreamName () (id : Guid) =
        sprintf "Foo-%s" <| id.ToString("N")
    type FooCmd = {
        Id : Guid
    }

    type FooEvent = {
        Id : Guid
    }

    let eventTypes = seq {
        yield typeof<FooEvent>
    }

    let fooHandlers () =    
        let cmdHandlers = seq {
            yield 
                AggregateActionBuilder.simpleHandler
                    StateBuilder.nullStateBuilder
                    (fun (cmd : FooCmd) -> 
                        ({ FooEvent.Id = cmd.Id }, metadataBuilder)
                    )    
                |> AggregateActionBuilder.buildCmd
        }

        let evtHandlers = Seq.empty

        Eventful.Aggregate.toAggregateDefinition getStreamName getStreamName id cmdHandlers evtHandlers

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate (fooHandlers ())
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

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId } () // first run
            |> TestSystem.runCommand { FooCmd.Id = thisId } () // second run

        let fooCount = afterRun.EvaluateState streamName thisId fooEventCounter

        fooCount |> should equal 1
  