namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open Xunit
open FsUnit.Xunit

module OnEventTests =

    type FooCmd = {
        Id : Guid
    }

    type FooEvent = {
        Id : Guid
    }

    type BarEvent = {
        Id : Guid
    }

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

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<BarEvent>
    }

    let getStreamName () (id : Guid) =
        sprintf "Foo-%s" <| id.ToString("N")

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

        Eventful.Aggregate.toAggregateDefinition getStreamName getStreamName id cmdHandlers evtHandlers

    let handlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate (fooHandlers ())
        |> addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty handlers

    let barEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.Empty "BarEventCount" 0
        |> StateBuilder.handler (fun (e:BarEvent) _ -> e.Id) (fun (s,e,m) -> s + 1)
        |> (fun s -> s :> IStateBuilder<int, TestMetadata, Guid>)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``FooEvent produces BarEvent`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName () thisId

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId }

        let barCount = afterRun.EvaluateState streamName thisId barEventCounter

        barCount |> should equal 1