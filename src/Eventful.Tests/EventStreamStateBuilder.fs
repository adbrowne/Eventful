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

    let rec addNextEvent nextEventNumber streamName (stateBuilder:StateBuilder<'TState>) (state:'TState) =
        eventStream {
            let! token = readFromStream streamName nextEventNumber
            match token with
            | Some token -> 
                let! value = readValue token typeof<WidgetAddedEvent> 
                let state' = stateBuilder.Run state value
                return! addNextEvent (nextEventNumber + 1) streamName stateBuilder state'
            | None -> return state
        }

    let streamStateBuilder streamName (stateBuilder:StateBuilder<'TState>) = eventStream {
            return! addNextEvent 0 streamName stateBuilder stateBuilder.InitialState
        }

    let stateBuilder = 
        StateBuilder.Empty List.empty
        |> StateBuilder.addHandler (fun s (e:WidgetAddedEvent) -> e.Name::s)

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

        let program = streamStateBuilder streamName stateBuilder
        let result = TestInterpreter.interpret program eventStoreState Map.empty

        result |> should equal ["Widget1"]

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

        let program = streamStateBuilder streamName stateBuilder
        let result = TestInterpreter.interpret program eventStoreState Map.empty

        result |> should equal ["Widget2";"Widget1"]

        ()