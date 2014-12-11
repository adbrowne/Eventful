namespace Eventful.Tests.Integration

open System
open Xunit
open Metrics
open Eventful
open Eventful.Raven
open Eventful.Tests

module RavenReplayProjectorTests = 

    [<Fact>]
    let ``Pump many events at Raven`` () : unit =
        use config = Metric.Config.WithHttpEndpoint("http://localhost:8083/")
        
        let documentStore = RavenProjectorTests.buildDocumentStore() :> Raven.Client.IDocumentStore 

        let streamCount = 10000
        let itemPerStreamCount = 100
        let totalEvents = streamCount * itemPerStreamCount
        let myEvents = Eventful.Tests.TestEventStream.sequentialNumbers streamCount itemPerStreamCount |> Seq.cache

        let streams = myEvents |> Seq.map (fun x -> Guid.Parse(x.StreamId)) |> Seq.distinct |> Seq.cache

        let myProjector = RavenProjectorTests.``Get Projector`` documentStore (async.Zero())

        let cancellationToken = Async.DefaultCancellationToken
        let cache = new System.Runtime.Caching.MemoryCache("RavenBatchWrite")

        let readQueue = new RavenReadQueue(documentStore, 100, 10000, 10, cancellationToken, cache)

        let projector =
            new RavenReplayProjector<SubscriberEvent>(documentStore, myProjector, RavenProjectorTests.testDatabase) 

        let sw = System.Diagnostics.Stopwatch.StartNew()
        for event in myEvents do
            projector.Enqueue event

        projector.ProcessQueuedItems()

        sw.Stop()

        consoleLog <| sprintf "Write time: %A ms" sw.ElapsedMilliseconds

        ()