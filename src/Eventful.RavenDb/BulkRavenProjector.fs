namespace Eventful.Raven

open System
open System.Threading
open System.Runtime.Caching

open Eventful
open Metrics

open FSharpx

open Raven.Client
open Raven.Abstractions.Data
open Raven.Json.Linq

module BulkRavenProjector =
    let create
        (
            databaseName: string,
            projectors : Projector<string, 'TMessage, IDocumentFetcher, ProcessAction> seq,
            cancellationToken : CancellationToken,
            onEventComplete : 'TMessage -> Async<unit>,
            documentStore:Raven.Client.IDocumentStore, 
            writeQueue : RavenWriteQueue,
            readQueue : RavenReadQueue,
            maxEventQueueSize : int,
            eventWorkers: int,
            workTimeout : TimeSpan option
        ) =
        let fetcher = new DocumentFetcher(documentStore, databaseName, readQueue) :> IDocumentFetcher

        let projectorsWithFetcher =
            BulkProjector.projectorsWithContext projectors fetcher

        let executor actions =
            writeQueue.Work databaseName actions

        let getPersistedPosition = async {
            let! (persistedLastComplete : ProjectedDocument<EventPosition> option) = fetcher.GetDocument RavenConstants.PositionDocumentKey |> Async.AwaitTask
            return persistedLastComplete |> Option.map((fun (doc,_,_) -> doc))
        }

        let writeUpdatedPosition position = async {
            let writeRequests =
                Write (
                    {
                        DocumentKey = RavenConstants.PositionDocumentKey
                        Document = position
                        Metadata = lazy(RavenOperations.emptyMetadata<EventPosition> documentStore)
                        Etag = null // just write this blindly
                    }, Guid.NewGuid())
                |> Seq.singleton

            let! writeResult = writeQueue.Work databaseName writeRequests

            return writeResult |> RavenWriteQueue.resultWasSuccess
        }

        BulkProjector<_, _, _>(
            "Raven-" + databaseName,
            projectorsWithFetcher,
            executor,
            cancellationToken,
            onEventComplete,
            getPersistedPosition,
            writeUpdatedPosition,
            maxEventQueueSize,
            eventWorkers,
            workTimeout,
            keyComparer = StringComparer.InvariantCultureIgnoreCase)
