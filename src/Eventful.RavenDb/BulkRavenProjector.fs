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
            documentStore:Raven.Client.IDocumentStore, 
            documentProcessor:DocumentProcessor<string, 'TMessage>,
            databaseName: string,
            maxEventQueueSize : int,
            eventWorkers: int,
            onEventComplete : 'TMessage -> Async<unit>,
            cancellationToken : CancellationToken,
            writeQueue : RavenWriteQueue,
            readQueue : RavenReadQueue,
            workTimeout : TimeSpan option
        ) =
        let fetcher = new DocumentFetcher(documentStore, databaseName, readQueue) :> IDocumentFetcher

        let eventProcessor =
            { MatchingKeys = documentProcessor.MatchingKeys
              Process = (fun (key, events) ->
                (fun () ->
                    async { 
                        let! writeRequests = documentProcessor.Process(key, fetcher, events).Invoke() |> Async.AwaitTask
                        return! writeQueue.Work databaseName writeRequests
                    }
                    |> Async.StartAsTask)
                |> (fun x -> Func<_> x) ) }

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

        BulkProjector<string, 'TMessage>(
            eventProcessor,
            databaseName,
            maxEventQueueSize,
            eventWorkers,
            onEventComplete,
            getPersistedPosition,
            writeUpdatedPosition,
            cancellationToken,
            workTimeout,
            keyComparer = StringComparer.InvariantCultureIgnoreCase)
