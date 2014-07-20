namespace Eventful.Raven

open Eventful
open System
open Metrics

type RavenWriteQueue 
    (
        documentStore:Raven.Client.IDocumentStore, 
        maxBatchSize : int,
        maxQueueSize : int,
        workerCount : int,
        cache : System.Runtime.Caching.MemoryCache
    ) =

    let batchWriteTracker = Metric.Histogram("BatchWriteSize", Unit.Items)
    let batchWritesMeter = Metric.Meter("BatchWrites" , Unit.Items)
    let batchWriteTime = Metric.Timer("WriteTime", Unit.None)
    let batchConflictsMeter = Metric.Meter("BatchConflicts", Unit.Items)

    let writeDocs databaseName (docs : seq<BatchWrite * AsyncReplyChannel<bool>>) = async {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let batchId = Guid.NewGuid()
        let originalDocMap = 
            docs
            |> Seq.collect (fun (writeRequests, callback) -> 
                writeRequests
                |> Seq.map(fun processAction ->
                    match processAction with
                    | Write ({ DocumentKey = key; Document = document; Metadata = metadata }, _) ->
                        let document = document
                        (key, Choice1Of2 (document, metadata.Force(), callback))
                    | Delete ({ DocumentKey = key }, _) ->
                        (key, Choice2Of2 ())
                )
            )
            |> Map.ofSeq

        let! result = BatchOperations.writeBatch documentStore databaseName (docs |> Seq.map fst)
        let writeSuccessful = 
            match result with
            | Some (batchResult, docs) ->
                batchWriteTracker.Update(batchResult.LongLength)
                batchWritesMeter.Mark()
                for docResult in batchResult do
                    match originalDocMap.[docResult.Key] with
                    | Choice1Of2 (doc, metadata, callback) ->
                        let cacheKey = RavenOperations.getCacheKey databaseName docResult.Key
                        cache.Set(cacheKey, (doc, metadata, docResult.Etag) :> obj, DateTimeOffset.MaxValue) |> ignore
                    | Choice2Of2 _ ->
                        let cacheKey = RavenOperations.getCacheKey databaseName docResult.Key
                        cache.Remove(cacheKey) |> ignore

                true
            | None ->
                batchConflictsMeter.Mark()
                for docRequest in originalDocMap |> Map.toSeq do
                    match docRequest with
                    | (docKey, Choice1Of2 (_, _, callback)) ->
                        let cacheKey = RavenOperations.getCacheKey databaseName docKey
                        cache.Remove(cacheKey) |> ignore
                    | _ -> ()
                false
        
        for (docs, callback) in docs do
            callback.Reply writeSuccessful

        sw.Stop()
        batchWriteTime.Record(sw.ElapsedMilliseconds, TimeUnit.Milliseconds)
    }

    let queue = new BatchingQueue<string, BatchWrite, bool>(maxBatchSize, maxQueueSize)

    let consumer = async {
        while true do
            let! (database, batch) = queue.Consume()
            do! (writeDocs database batch)
    }

    let blah = 
        for _ in [1..workerCount] do
            consumer |> Async.StartAsTask |> ignore
        ()
    
    member x.Work = queue.Work