namespace Eventful.Testing

open Xunit
open FsUnit.Xunit
open System
open Eventful
open Eventful.EventStream

module EventStreamStateBuilder = 

    type WidgetAddedEvent = {
        Name : string
    }

    let streamStateBuilder streamName (stateBuilder:StateBuilder<'TState>) = eventStream {
            let! token = readFromStream streamName 0
            let! value = readValue token typeof<WidgetAddedEvent>
            let state = stateBuilder.InitialState
            let state = stateBuilder.Run state value
            return state
        }

    [<Fact>]
    let ``Can build up stream state using test interpreter`` () : unit =
        let stateBuilder = 
            StateBuilder.Empty List.empty
            |> StateBuilder.addHandler (fun s (e:WidgetAddedEvent) -> e.Name::s)

        let newMetadata () =
            { 
                MessageId = (Guid.NewGuid()) 
                SourceMessageId = (Guid.NewGuid()) 
            }

        let streamName = "TestStream-1"
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent (streamName, { Name = "Widget1" }, newMetadata())

        let program = streamStateBuilder streamName stateBuilder
        let result = TestInterpreter.interpret program eventStoreState Map.empty

        result |> should equal ["Widget1"]

        ()