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
        cache : System.Runtime.Caching.MemoryCache
    ) =

    let batchReadBatchSizeHistogram = Metric.Histogram("RavenReadQueue Batch Size", Unit.Items)
    let batchReadTimer = Metric.Timer("RavenReadQueue Timer", Unit.None)

    let readDocs databaseName (docs : seq<(seq<GetDocRequest> * AsyncReplyChannel<seq<GetDocResponse>>)>) : Async<unit>  = 

        let request = 
            docs
            |> Seq.collect (fun i -> fst i)
            |> List.ofSeq

        batchReadBatchSizeHistogram.Update(int64 request.Length)
           
        async {
            let timer = startNanoSecondTimer()
            let! getResult = 
                RavenOperations.getDocuments documentStore cache databaseName request

            let resultMap =
                getResult
                |> Seq.map(fun (docId, t, response) -> (docId, (docId, t, response)))
                |> Map.ofSeq

            for (request, reply) in docs do
                let responses =
                    request
                    |> Seq.map (fun (k,_) -> resultMap.Item k)

                reply.Reply responses

            batchReadTimer.Record(timer(), TimeUnit.Nanoseconds)
            return ()
        }
        

    let queue = new BatchingQueue<string, seq<string * Type>, seq<GetDocResponse>>(maxBatchSize, maxQueueSize)

    let consumer = async {
        while true do
            let! (database, batch) = queue.Consume()
            do! (readDocs database batch)
    }

    let startConsumers = 
        for i in [1..workerCount] do
            let taskName = sprintf "Read Queue Worker %d" i
            runAsyncAsTask taskName cancellationToken consumer  |> ignore
        ()
    
    member x.Work = queue.Work