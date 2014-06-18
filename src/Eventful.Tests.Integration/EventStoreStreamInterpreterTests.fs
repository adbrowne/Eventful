namespace Eventful.Tests.Integration

open Xunit
open System
open EventStore.ClientAPI
open FsUnit.Xunit
open Eventful
open Eventful.EventStream
open Eventful.EventStore

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
                return "Andrew"
            }

            let interpreted = EventStreamInterpreter.interpret client RunningTests.esSerializer writeARandomEvent

            let! result = interpreted

            result |> should equal "Andrew"
        } |> Async.RunSynchronously

