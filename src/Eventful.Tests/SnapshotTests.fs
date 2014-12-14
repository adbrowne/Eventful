namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing
open FSharpx
open FSharpx.Collections

open Xunit
open FsUnit.Xunit
open FsCheck.Xunit

module SnapshotTests =
    open EventSystemTestCommon

    let metadataBuilder sourceMessageId = { 
        SourceMessageId = sourceMessageId 
        AggregateType =  "TestAggregate" }

    type FooCmd = {
        Id : Guid
        Value : int
    }

    type FooEvent = {
        Id : Guid
        Value : int
        RunningTotal : int
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
    }

    let runningTotalBuilder = 
        StateBuilder.Empty "RunningTotal" 0
        |> StateBuilder.aggregateStateHandler (fun (s,e:FooEvent,(_:TestMetadata)) -> s + e.Value)
        |> StateBuilder.toInterface

    let fooHandlers : AggregateDefinition<Guid, _, _, _, _, _> =    
        let cmdHandlers = seq {
            yield 
                cmdHandlerS
                    runningTotalBuilder
                    (fun runningTotal (cmd : FooCmd) -> 
                        { 
                            FooEvent.Id = cmd.Id
                            Value = cmd.Value
                            RunningTotal = runningTotal + cmd.Value } :> IEvent)
                |> AggregateActionBuilder.buildCmd
        }

        let evtHandlers = Seq.empty

        Eventful.Aggregate.toAggregateDefinition 
            "TestAggregate"
            TestMetadata.GetUniqueId
            (fun _ _ -> "Foo") 
            (fun _ _ -> "Foo") 
            cmdHandlers
            evtHandlers

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate fooHandlers
        |> StandardConventions.addEventTypes eventTypes

    let testSystemWithSnapshots = TestSystem.Empty (konst UnitEventContext) handlers
    let testSystemWithoutSnapshots = TestSystem.EmptyNoSnapshots (konst UnitEventContext) handlers

    [<Property>]
    [<Trait("category", "unit")>]
    let ``Behavior is the same with and without snapshots`` (cmds : FooCmd list) =
        let getEvents testSystem = 
            cmds
            |> List.fold (fun (s : TestSystem<_,_,_,_,_>) cmd -> s.RunCommand cmd (Guid.NewGuid())) testSystem
            |> TestSystem.getStreamEvents "Foo"
            |> Vector.map (function
                | Event eventData ->
                    eventData.Body |> Vector.singleton
                | _ -> Vector.empty)
            |> Vector.flatten
            |> List.ofSeq

        getEvents testSystemWithSnapshots |> should equal (getEvents testSystemWithoutSnapshots)