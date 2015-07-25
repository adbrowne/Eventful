namespace Eventful.Raven

open Eventful
open System
open Metrics
open System.Threading

type RavenWriteQueueResult = Choice<unit,System.Exception>

type RavenWriteQueue 
    (
        documentStore:Raven.Client.IDocumentStore, 
        maxBatchSize : int,
        maxQueueSize : int,
        workerCount : int,
        cancellationToken : CancellationToken,
        cache : RavenMemoryCache
    ) =

    let log = EventfulLog.ForContext "Eventful.RavenWriteQueue"

    let batchWriteTracker = Metric.Histogram("RavenWriteQueue Batch Size", Unit.Items)
    let batchWriteTime = Metric.Timer("RavenWriteQueue Timer", Unit.None)
    let batchWriterThroughput = Metric.Meter("RavenWriteQueue Documents Written", Unit.Items)
    let batchConflictsMeter = Metric.Meter("RavenWriteQueue Conflicts", Unit.Items)

    let writeDocs databaseName (docs : seq<BatchWrite * AsyncReplyChannel<RavenWriteQueueResult>>) = async {
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
            | Choice1Of2 (batchResult, docs) ->
                batchWriteTracker.Update(batchResult.LongLength)
                batchWriterThroughput.Mark(batchResult.LongLength)
                for docResult in batchResult do
                    match originalDocMap.[docResult.Key] with
                    | Choice1Of2 (doc, metadata, callback) ->
                        cache.Set databaseName docResult.Key (doc, metadata, docResult.Etag)
                    | Choice2Of2 _ ->
                        cache.Remove databaseName docResult.Key

                Choice1Of2 ()
            | Choice2Of2 e ->
                batchConflictsMeter.Mark()
                for (docKey, _) in originalDocMap |> Map.toSeq do
                    cache.Remove databaseName docKey
                Choice2Of2 e
        
        for (docs, callback) in docs do
            callback.Reply writeSuccessful

        sw.Stop()
        batchWriteTime.Record(sw.ElapsedMilliseconds, TimeUnit.Milliseconds)
    }

    let queue = new BatchingQueue<string, BatchWrite, RavenWriteQueueResult>(maxBatchSize, maxQueueSize)

    let consumer = async {
        while true do
            let! (database, batch) = queue.Consume()
            try
                do! (writeDocs database batch)
            with | e ->
                log.Debug(e, "Exception on write")
    }

    let startConsumers = 
        for i in [1..workerCount] do
            let taskName = sprintf "Write Queue Worker %d" i
            runAsyncAsTask taskName cancellationToken consumer |> ignore
        ()
    
    member x.Work = queue.Work

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RavenWriteQueue =
    let resultWasSuccess (result : RavenWriteQueueResult) = 
        match result with
        | Choice1Of2 _ -> true
        | _ -> false