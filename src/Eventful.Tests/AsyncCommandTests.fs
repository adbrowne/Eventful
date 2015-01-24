namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open FSharpx

open Xunit
open FsUnit.Xunit

module AsyncCommandTests =
    open EventSystemTestCommon

    type CommandContextWithAsyncService = {
        ContextId : Guid
        GetAsyncValue : Async<int>
    }

    let metadataBuilder sourceMessageId = { 
        SourceMessageId = sourceMessageId 
        AggregateType =  "TestAggregate" }

    type FooCmd = {
        Id : Guid
    }

    type FooEvent = {
        Id : Guid
        Value : int
    }

    let eventTypes = seq {
        yield typeof<FooEvent>
    }

    let lastFooValueBuilder : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.Empty "lastFooValue" -1
        |> StateBuilder.handler (fun (e:FooEvent) m -> e.Id) (fun (s,e:FooEvent,_) -> e.Value)
        |> StateBuilder.toInterface

    let cmdHandlerAsync f =
        AggregateActionBuilder.fullHandlerAsync
            MagicMapper.magicGetCmdId<_>
            StateBuilder.nullStateBuilder
            (fun _ (cmdContext : CommandContextWithAsyncService) cmd -> async {
                let! events = async {
                    let! result = f cmdContext cmd 
                    return
                        result
                        |> (fun evt -> (evt :> obj, metadataBuilder (cmdContext.ToString() |> Some)))
                        |> Seq.singleton 
                }

                return
                    Choice1Of2 events
            })

    let fooHandlers =    
        let cmdHandlers = seq {
            yield 
                cmdHandlerAsync
                    (fun cmdContext (cmd : FooCmd) -> async {
                        let! value = cmdContext.GetAsyncValue
                        return { FooEvent.Id = cmd.Id; Value = value } 
                    })
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
        EventfulHandlers.empty (konst "TestAggregate")
        |> EventfulHandlers.addAggregate fooHandlers
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst UnitEventContext) handlers

    let fooEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:FooEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can get async value in command`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName UnitEventContext  thisId

        // some unique id that can make the command processing idempotent
        let commandId = Guid.NewGuid() 

        let asyncValue = 4
        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId } { ContextId =  commandId; GetAsyncValue = Async.returnM asyncValue }// first run

        let lastValue = afterRun.EvaluateState streamName thisId lastFooValueBuilder

        lastValue |> should equal asyncValue