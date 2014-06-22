namespace Eventful.Tests.Integration
module RavenDbPlay =

    open Xunit
    open Raven.Client.Document
    open Raven.Abstractions.Commands
    open Raven.Json.Linq
    open metrics

    type TestType = TestType

    [<CLIMutable>]
    type Person = {
        FirstName : string
        LastName : string
        Age: int
        Description: string
    }

    [<Fact>]
    let ``Insert bulk documents`` () : unit = 
        let eventsMeter = Metrics.Meter(typeof<TestType>, "insert", "inserts", TimeUnit.Seconds)
        Metrics.EnableConsoleReporting(10L, TimeUnit.Seconds)

        use store = new DocumentStore(Url = "http://localhost:8080")
        store.Initialize() |> ignore

        let personJson = RavenJObject.FromObject({ FirstName = "Andrew"; LastName = "Browne"; Age = 31; Description = "Some boring statement"})
        let metadata = new RavenJObject()
        metadata.Add("Raven-Entity-Name", new RavenJValue("Person"))
        metadata.Add("Raven-Clr-Type", new RavenJValue(typeof<Person>.FullName))

        let createPutCmd n =
            let cmd = new PutCommandData()
            cmd.Key <- "Person/" + System.Guid.NewGuid().ToString()
            cmd.Document <- personJson
            cmd.Metadata <- metadata
            cmd :> ICommandData

        let insertLots () = async {
                use dbStuff = store.AsyncDatabaseCommands.ForDatabase("BulkTestDb")
                let! result = dbStuff.BatchAsync([1..500] |> List.map createPutCmd |> List.toArray) |> Async.AwaitTask
                let! result = dbStuff.BatchAsync([1..500] |> List.map createPutCmd |> List.toArray) |> Async.AwaitTask
                eventsMeter.Mark 1000L
                ()
            } 

        async {
            for _ in [1..20] do
                do! Async.Parallel [insertLots(); insertLots(); insertLots(); insertLots(); insertLots()] |> Async.Ignore

        } |> Async.RunSynchronously