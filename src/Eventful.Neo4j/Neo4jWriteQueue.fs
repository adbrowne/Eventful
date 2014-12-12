namespace Eventful.Neo4j

open System
open System.Threading

open Eventful

open Metrics
open Neo4jClient

// TODO: Cache

type Neo4jWriteQueue 
    (
        graphClient : ICypherGraphClient,
        maxBatchSize : int,
        maxQueueSize : int,
        workerCount : int,
        cancellationToken : CancellationToken
    ) =

    let log = EventfulLog.ForContext "Eventful.Neo4jWriteQueue"

    let batchWriteTracker = Metric.Histogram("Neo4jWriteQueue Batch Size", Unit.Items)
    let batchWriteTime = Metric.Timer("Neo4jWriteQueue Timer", Unit.None)
    let batchWriterThroughput = Metric.Meter("Neo4jWriteQueue Nodes Written", Unit.Items)
    let batchConflictsMeter = Metric.Meter("Neo4jWriteQueue Conflicts", Unit.Items)

    let processActions graphName (actionBatches : seq<seq<GraphAction> * AsyncReplyChannel<Choice<unit, exn>>>) = async {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        
        let! result = Operations.writeBatch graphClient graphName (actionBatches |> Seq.map fst)
        
        match result with
        | Choice1Of2 _ ->
            let batchSize =
                actionBatches
                |> Seq.collect fst
                |> Seq.length

            batchWriteTracker.Update(int64 batchSize)
            batchWriterThroughput.Mark(int64 batchSize)
        | Choice2Of2 e ->
            batchConflictsMeter.Mark()
        
        for (actions, callback) in actionBatches do
            callback.Reply result

        sw.Stop()
        batchWriteTime.Record(sw.ElapsedMilliseconds, TimeUnit.Milliseconds)
    }

    let queue = new BatchingQueue<string, GraphAction seq, Choice<unit, exn>>(maxBatchSize, maxQueueSize)

    let consumer = async {
        while true do
            let! (graphName, batch) = queue.Consume()
            try
                do! (processActions graphName batch)
            with | e ->
                log.Debug(e, "Exception on write")
    }

    let startConsumers = 
        for i in [1..workerCount] do
            let taskName = sprintf "Neo4j Write Queue Worker %d" i
            runAsyncAsTask taskName cancellationToken consumer |> ignore
        ()
    
    member x.Work = queue.Work
