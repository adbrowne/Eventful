namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing
open FSharpx
open FSharpx.Collections

open Xunit
open Swensen.Unquote
open FsCheck.Xunit

module StreamMetadataTests = 
    open EventSystemTestCommon

    let metadataBuilder sourceMessageId = { 
        SourceMessageId = sourceMessageId 
        AggregateType =  "TestAggregate" }

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

    let fooHandlers streamMetadata : AggregateDefinition<Guid,_,_,_,_>  =    
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> 
                        { FooEvent.Id = cmd.Id } :> IEvent)
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
        |> Aggregate.withStreamMetadata streamMetadata

    let handlers streamMetadata =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate (fooHandlers streamMetadata)
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem streamMetadata = TestSystem.Empty (konst UnitEventContext) (handlers streamMetadata)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Metadata is written with the first event`` () =
        let fooId = Guid.NewGuid()
        let result = 
            emptyTestSystem { EventStreamMetadata.Default with MaxCount = Some 1 }
            |> TestSystem.runCommand { FooCmd.Id = fooId } (Guid.NewGuid())
            |> TestSystem.getStreamMetadata "Foo"

        result =? Some (Vector.singleton { EventStreamMetadata.Default with MaxCount = Some 1 })

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Metadata is not written again on the second event`` () =
        let fooId = Guid.NewGuid()
        let result = 
            emptyTestSystem { EventStreamMetadata.Default with MaxCount = Some 1 }
            |> TestSystem.runCommand { FooCmd.Id = fooId } (Guid.NewGuid())
            |> TestSystem.runCommand { FooCmd.Id = fooId } (Guid.NewGuid())
            |> TestSystem.getStreamMetadata "Foo"

        result =? Some (Vector.singleton { EventStreamMetadata.Default with MaxCount = Some 1 })

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Metadata is not written if it is default`` () =
        let fooId = Guid.NewGuid()
        let result = 
            emptyTestSystem EventStreamMetadata.Default
            |> TestSystem.runCommand { FooCmd.Id = fooId } (Guid.NewGuid())
            |> TestSystem.getStreamMetadata "Foo"

        result =? None

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Stream with max count does not return old events`` () =
        let fooId = Guid.NewGuid()
        let maxCount = 5
        let eventCount = 10
        let emptySystem = emptyTestSystem { EventStreamMetadata.Default with MaxCount = Some maxCount }
        let system =
            Seq.repeat { FooCmd.Id = fooId }
            |> Seq.take eventCount
            |> Seq.fold (fun system command -> system |> TestSystem.runCommand command (Guid.NewGuid())) emptySystem
        
        let eventShouldBeDropped = TestEventStore.tryGetEvent system.AllEvents "Foo" (eventCount - maxCount - 1)
        eventShouldBeDropped =? None

        let eventShouldNotBeDropped = TestEventStore.tryGetEvent system.AllEvents "Foo" (eventCount - maxCount)
        eventShouldNotBeDropped.IsSome =? true
