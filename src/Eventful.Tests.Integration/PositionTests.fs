namespace Eventful.Tests.Integration

open Xunit
open EventStore.ClientAPI
open System
open System.IO
open Newtonsoft.Json
open FsUnit.Xunit
open Eventful
open Eventful.EventStore

module PositionTests = 
    [<Fact>]
    let ``Set and get position`` () : unit = 
        async {
            let commitPosition = 1234L
            let preparePosition = 5678L
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! ProcessingTracker.setPosition client { Commit = commitPosition; Prepare = preparePosition}

            let! position = ProcessingTracker.readPosition client

            match position with
            | Some { Commit = commit; Prepare = prepare } when commit = commitPosition && prepare = preparePosition -> Assert.True(true)
            | p -> Assert.True(false, (sprintf "Unexpected position %A" p))
        } |> Async.RunSynchronously