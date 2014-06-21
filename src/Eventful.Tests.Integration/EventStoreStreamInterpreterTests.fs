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

    let event = { Name = "Andrew Browne" }
    let metadata = { SourceMessageId = System.Guid.NewGuid(); MessageId = System.Guid.NewGuid() }

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Basic commands and events`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let run program =
                EventStreamInterpreter.interpret client RunningTests.esSerializer program

            let stream = "MyStream-" + (newId())

            let! writeResult = 
                eventStream {
                    let writes : seq<EventStreamEvent> = Seq.singleton (EventStreamEvent.Event (event :> obj, metadata))
                    let! ignore = writeToStream stream EventStore.ClientAPI.ExpectedVersion.EmptyStream writes
                    return "Write Complete"
                } |> run

            writeResult |> should equal "Write Complete"

            let! readResult =
                eventStream {
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
                } |> run

            readResult |> should equal (Some "Andrew Browne")

        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Wrong Expected Version is Returned`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let run program =
                EventStreamInterpreter.interpret client RunningTests.esSerializer program

            let stream = "MyStream-" + (newId())

            let wrongExpectedVersion = 10
            let! writeResult = 
                eventStream {
                    let writes : seq<EventStreamEvent> = Seq.singleton (EventStreamEvent.Event(event :> obj, metadata))
                    return! writeToStream stream wrongExpectedVersion writes
                } |> run

            writeResult |> should equal WrongExpectedVersion
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Can create link`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let run program =
                EventStreamInterpreter.interpret client RunningTests.esSerializer program

            let sourceStream = "SourceStream-" + (newId())
            let stream = "MyStream-" + (newId())

            let! writeLink = 
                eventStream {
                    let writes = Seq.singleton (EventStreamEvent.Event(event :> obj, metadata))
                    let! ignore = writeToStream sourceStream EventStore.ClientAPI.ExpectedVersion.EmptyStream writes
                    let! ignore = EventStream.writeLink stream EventStore.ClientAPI.ExpectedVersion.EmptyStream sourceStream 0 metadata
                    return ()
                } |> run

            let! readResult =
                eventStream {
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
                } |> run

            readResult |> should equal (Some event.Name)
        } |> Async.RunSynchronously