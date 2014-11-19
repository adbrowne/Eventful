namespace Eventful.Tests

open Xunit
open FsUnit.Xunit
open System
open Eventful
open Eventful.Testing
open Eventful.EventStream
open FSharpx.Collections

module EventStreamStateBuilder = 

    type WidgetAddedEvent = {
        Id : Guid
        Name : string
    }

    let stateBuilder =
        StateBuilder.Empty "names" List.empty
        |> StateBuilder.handler (fun (e:WidgetAddedEvent) (m:TestMetadata) -> e.Id) (fun (s, (e:WidgetAddedEvent), _) -> e.Name::s)
        |> (fun x -> x :> IStateBuilder<string list, TestMetadata, Guid>)

    let runProgram eventStoreState p = 
        TestInterpreter.interpret p eventStoreState PersistentHashMap.empty PersistentHashMap.empty Map.empty Vector.empty |> snd

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from single event`` () : unit =
        let newMetadata () =
            { 
                SourceMessageId = (Guid.NewGuid().ToString())
                AggregateType =  "TestAggregate" 
            }

        let streamName = "TestStream-1"
        let widgetId = Guid.NewGuid()
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1"; Id = widgetId }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> AggregateStateBuilder.toStreamProgram streamName widgetId
        let (count, state) = runProgram eventStoreState program

        count |> should equal 1
        stateBuilder.GetState state |> should equal (["Widget1"])
        ()

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from multiple events`` () : unit =
        let newMetadata () =
            { 
                SourceMessageId = (Guid.NewGuid().ToString())
                AggregateType =  "TestAggregate" 
            }

        let streamName = "TestStream-1"
        let widgetId = Guid.NewGuid()
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget2"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> AggregateStateBuilder.toStreamProgram streamName widgetId

        let (count, state) = runProgram eventStoreState program

        count |> should equal 2
        stateBuilder.GetState state |> should equal (["Widget2";"Widget1"])

        ()