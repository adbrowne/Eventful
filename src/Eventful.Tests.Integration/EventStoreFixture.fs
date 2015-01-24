namespace Eventful.Tests.Integration

open System

// just an event store connection
type EventStoreFixture () =
    let eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore ()

    member x.Connection = eventStoreProcess.Connection

    interface IDisposable with
        member this.Dispose () =
            (eventStoreProcess :> IDisposable).Dispose()
