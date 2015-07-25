namespace Eventful.Raven

open Eventful
open System
open Metrics
open System.Threading

type RavenReadQueue 
    (
        documentStore:Raven.Client.IDocumentStore, 
        maxBatchSize : int,
        maxQueueSize : int,
        workerCount : int,
        cancellationToken : CancellationToken,
        cache : RavenMemoryCache
    ) =

    let log = createLogger "Eventful.RavenReadQueue"

    let batchReadBatchSizeHistogram = Metric.Histogram("RavenReadQueue Batch Size", Unit.Items)
    let batchReadTimer = Metric.Timer("RavenReadQueue Timer", Unit.None)
    let batchReadThroughput = Metric.Meter("RavenReadQueue Documents Read", Unit.Items)

    let partition (counts : int seq) (xs : 'a seq) : 'a list list =
        [
            let enumerator = xs.GetEnumerator()
            for count in counts ->
                [
                    for i in 1 .. count ->
                        let hasMore = enumerator.MoveNext()
                        assert hasMore
                        enumerator.Current
                ]

            let hasMore = enumerator.MoveNext()
            assert (not hasMore)
        ]

    let readDocs databaseName (docs : seq<(seq<GetDocRequest> * AsyncReplyChannel<seq<GetDocResponse>>)>) : Async<unit> =
        let requestsWithReplyChannels =
            docs
            |> Seq.map (fun (batch, replyChannel) -> (List.ofSeq batch, replyChannel))
            |> List.ofSeq

        let requests = 
            requestsWithReplyChannels
            |> List.collect fst

        let documentCount = int64 requests.Length
        batchReadBatchSizeHistogram.Update(documentCount)
        batchReadThroughput.Mark(documentCount)
           
        async {
            let timer = startNanoSecondTimer()
            let! getResult = RavenOperations.getDocuments documentStore cache databaseName requests

            let batchSizes = requestsWithReplyChannels |> Seq.map (fst >> List.length)
            let partitionedResults = getResult |> partition batchSizes
            let replyChannels = requestsWithReplyChannels |> Seq.map snd

            Seq.zip replyChannels partitionedResults
            |> Seq.iter (fun (replyChannel, results) -> replyChannel.Reply results)

            batchReadTimer.Record(timer(), TimeUnit.Nanoseconds)
            return ()
        }
        

    let queue = new BatchingQueue<string, seq<GetDocRequest>, seq<GetDocResponse>>(maxBatchSize, maxQueueSize)

    let consumer = async {
        while true do
            let! (database, batch) = queue.Consume()
            try
                do! (readDocs database batch)
            with | e ->
                log.DebugWithException <| lazy("Exception on read",e)
    }

    let startConsumers = 
        for i in [1..workerCount] do
            let taskName = sprintf "Read Queue Worker %d" i
            runAsyncAsTask taskName cancellationToken consumer  |> ignore
        ()
    
    member x.Work = queue.Work