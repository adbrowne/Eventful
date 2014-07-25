namespace Eventful.Raven

open Eventful
open System
open Metrics
open System.Threading

type RavenWriteQueue 
    (
        documentStore:Raven.Client.IDocumentStore, 
        maxBatchSize : int,
        maxQueueSize : int,
        workerCount : int,
        cancellationToken : CancellationToken,
        cache : System.Runtime.Caching.MemoryCache
    ) =

    let log = Common.Logging.LogManager.GetLogger("Eventful.RavenReadQueue")

    let batchWriteTracker = Metric.Histogram("RavenWriteQueue Batch Size", Unit.Items)
    let batchWriteTime = Metric.Timer("RavenWriteQueue Timer", Unit.None)
    let batchConflictsMeter = Metric.Meter("RavenWriteQueue Conflicts", Unit.Items)

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
                    | Custom c ->
                        (c.Key, Choice2Of2 ())
                )
            )
            |> Map.ofSeq

        let! result = BatchOperations.writeBatch documentStore databaseName (docs |> Seq.map fst)
        let writeSuccessful = 
            match result with
            | Some (batchResult, docs) ->
                batchWriteTracker.Update(batchResult.LongLength)
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
            try
                do! (writeDocs database batch)
            with | e ->
                if log.IsDebugEnabled then
                    log.Debug("Exception on write",e)
    }

    let startConsumers = 
        for i in [1..workerCount] do
            let taskName = sprintf "Write Queue Worker %d" i
            runAsyncAsTask taskName cancellationToken consumer |> ignore
        ()
    
    member x.Work = queue.Work