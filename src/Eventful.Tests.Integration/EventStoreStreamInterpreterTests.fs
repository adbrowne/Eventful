namespace Eventful.Tests.Integration

open Xunit
open System
open EventStore.ClientAPI
open FsUnit.Xunit
open Eventful.EventStream
open Eventful.EventStore

module EventStoreStreamInterpreterTests = 

    type MyEvent = {
        Name : string
    }

    [<Fact>]
    let ``Basic commands and events`` () : unit =
        async {
            let! connection = RunningTests.getConnection()

            let stream = "MyStream-" + (System.Guid.NewGuid())

            let event = { Name = "Andrew Browne" }
            let metadata = { SourceMessageId = Guid.NewGuid(); MessageId = Guid.NewGuid() }
            let writeARandomEvent = eventStream {
                do! writeToStream stream 0 event metadata
            }

            let interpreted = EventStreamInterpreter.interpret 
        } |> Async.RunSynchronously

