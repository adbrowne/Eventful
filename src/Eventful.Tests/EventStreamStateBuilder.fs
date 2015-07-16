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
        TestInterpreter.interpret p eventStoreState true PersistentHashMap.empty PersistentHashMap.empty Map.empty Vector.empty |> snd

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from single event`` () : unit =
        let newMetadata () =
            { 
                SourceMessageId = None
                AggregateType =  "TestAggregate" 
            }

        let streamName = "TestStream-1"
        let widgetId = Guid.NewGuid()
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1"; Id = widgetId }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> AggregateStateBuilder.toStreamProgram streamName widgetId
        let snapshot = runProgram eventStoreState program

        snapshot.LastEventNumber |> should equal 0
        stateBuilder.GetState snapshot.State |> should equal (["Widget1"])
        ()

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from multiple events`` () : unit =
        let newMetadata () =
            { 
                SourceMessageId = None
                AggregateType =  "TestAggregate" 
            }

        let streamName = "TestStream-1"
        let widgetId = Guid.NewGuid()
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget2"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> AggregateStateBuilder.toStreamProgram streamName widgetId

        let snapshot = runProgram eventStoreState program

        snapshot.LastEventNumber |> should equal 1
        stateBuilder.GetState snapshot.State |> should equal (["Widget2";"Widget1"])

        ()

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from stream with max count`` () : unit =
        let newMetadata () =
            { 
                SourceMessageId = None
                AggregateType =  "TestAggregate" 
            }

        let streamName = "TestStream-1"
        let widgetId = Guid.NewGuid()
 
        let eventStoreState = 
            TestEventStore.empty
            |> TestEventStore.setStreamMetadata streamName { EventStreamMetadata.Default with MaxCount = Some 1 }
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget2"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget3"; Id = widgetId  }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> AggregateStateBuilder.toStreamProgram streamName widgetId

        let snapshot = runProgram eventStoreState program

        snapshot.LastEventNumber |> should equal 2
        stateBuilder.GetState snapshot.State |> should equal (["Widget3"])
