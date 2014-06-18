namespace Eventful.Tests.Integration

open Xunit
open System
open EventStore.ClientAPI
open FsUnit.Xunit
open Eventful
open Eventful.EventStream
open Eventful.EventStore

open FSharpx.Option

module EventStoreStreamInterpreterTests = 

    type MyEvent = {
        Name : string
    }

    let newId () : string =
        System.Guid.NewGuid().ToString()

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Basic commands and events`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let stream = "MyStream-" + (newId())

            let event = { Name = "Andrew Browne" }
            let metadata = { SourceMessageId = System.Guid.NewGuid(); MessageId = System.Guid.NewGuid() }

            let writeARandomEvent = eventStream {
                do! writeToStream stream EventStore.ClientAPI.ExpectedVersion.EmptyStream event metadata
                return "Write Complete"
             
            }

            let! writeResult = EventStreamInterpreter.interpret client RunningTests.esSerializer writeARandomEvent
            writeResult |> should equal "Write Complete"

            let readEventProgram = eventStream {
                let! item = readFromStream stream EventStore.ClientAPI.StreamPosition.Start
                return!
                    match item with
                    | Some x -> 
                        eventStream { 
                            let! objValue = readValue x typeof<MyEvent>
                            let value = objValue :?> MyEvent
                            return Some value.Name
                        }
                    | None -> eventStream { return None } 
            }

            let! readResult = EventStreamInterpreter.interpret client RunningTests.esSerializer readEventProgram
            readResult |> should equal (Some "Andrew Browne")

        } |> Async.RunSynchronously