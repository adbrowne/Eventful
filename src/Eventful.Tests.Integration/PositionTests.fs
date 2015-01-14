namespace Eventful.Tests.Integration

open Xunit
open Eventful
open Eventful.EventStore

type PositionTests () = 

    let mutable connection : EventStore.ClientAPI.IEventStoreConnection = null

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Set and get position`` () : unit = 
        async {
            let commitPosition = 1234L
            let preparePosition = 5678L
            let client = new EventStoreClient(connection)

            let streamId = "SomeStream"
            let! version = ProcessingTracker.setPosition client streamId { Commit = commitPosition; Prepare = preparePosition}

            let! position = ProcessingTracker.readPosition client streamId

            match position with
            | { Commit = commit; Prepare = prepare } when commit = commitPosition && prepare = preparePosition -> Assert.True(true)
            | p -> Assert.True(false, (sprintf "Unexpected position %A" p))
        } |> Async.RunSynchronously

    interface Xunit.IUseFixture<EventStoreFixture> with
        member x.SetFixture(fixture) =
            connection <- fixture.Connection