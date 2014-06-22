namespace Eventful.Testing

open Xunit
open FsUnit.Xunit
open System
open Eventful
open Eventful.EventStream
open FSharpx.Collections

module EventStreamStateBuilder = 

    type WidgetAddedEvent = {
        Name : string
    }

    let stateBuilder =
        StateBuilder.Empty List.empty
        |> StateBuilder.addHandler (fun s (e:WidgetAddedEvent) -> e.Name::s)

    let runProgram eventStoreState p = 
        TestInterpreter.interpret p eventStoreState Map.empty Vector.empty |> snd

    [<Fact>]
    let ``Can build state from single event`` () : unit =
        let newMetadata () =
            { 
                MessageId = (Guid.NewGuid()) 
                SourceMessageId = (Guid.NewGuid()) 
            }

        let streamName = "TestStream-1"
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent (streamName, { Name = "Widget1" }, newMetadata())

        let program = stateBuilder |> StateBuilder.toStreamProgram streamName
        let result = runProgram eventStoreState program

        result |> should equal (Some ["Widget1"])

        ()

    [<Fact>]
    let ``Can build state from multiple events`` () : unit =
        let newMetadata () =
            { 
                MessageId = (Guid.NewGuid()) 
                SourceMessageId = (Guid.NewGuid()) 
            }

        let streamName = "TestStream-1"
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent (streamName, { Name = "Widget1" }, newMetadata())
            |> TestEventStore.addEvent (streamName, { Name = "Widget2" }, newMetadata())

        let program = stateBuilder |> StateBuilder.toStreamProgram streamName

        let result = runProgram eventStoreState program

        result |> should equal (Some ["Widget2";"Widget1"])

        ()