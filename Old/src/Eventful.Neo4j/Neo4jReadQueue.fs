namespace Eventful.Neo4j
//
//open System
//open System.Threading
//
//open Eventful
//
//open Neo4jClient
//open Metrics
//
//// TODO: Cache
//
//type Neo4jReadQueue
//    (
//        graphClient : ICypherGraphClient,
//        maxBatchSize : int,
//        maxQueueSize : int,
//        workerCount : int,
//        cancellationToken : CancellationToken
//    ) =
//    
//    let log = Common.Logging.LogManager.GetLogger("Eventful.Neo4jReadQueue")
//
//    let batchReadBatchSizeHistogram = Metric.Histogram("Neo4jReadQueue Batch Size", Unit.Items)
//    let batchReadTimer = Metric.Timer("Neo4jReadQueue Timer", Unit.None)
//    let batchReadThroughput = Metric.Meter("Neo4jReadQueue Documents Read", Unit.Items)
//
//    let processActions (docs : seq<(seq<NodeId * Type> * AsyncReplyChannel<seq<GetDocResponse>>)>) : Async<unit>  = 
//
//        let request = 
//            docs
//            |> Seq.collect (fun i -> fst i)
//            |> List.ofSeq
//
//        let documentCount = int64 request.Length
//        batchReadBatchSizeHistogram.Update(documentCount)
//        batchReadThroughput.Mark(documentCount)
//           
//        async {
//            let sw = System.Diagnostics.Stopwatch.StartNew()
//            let! getResult = 
//                RavenOperations.getDocuments documentStore cache databaseName request
//
//            let resultMap =
//                getResult
//                |> Seq.map(fun (docId, t, response) -> (docId, (docId, t, response)))
//                |> Map.ofSeq
//
//            for (request, reply) in docs do
//                let responses =
//                    request
//                    |> Seq.map (fun (k,_) -> resultMap.Item k)
//
//                reply.Reply responses
//
//            batchReadTimer.Record(timer(), TimeUnit.Nanoseconds)
//            return ()
//        }
//        
//
//    let queue = new BatchingQueue<string, seq<string * Type>, seq<GetDocResponse>>(maxBatchSize, maxQueueSize)
//
//    let consumer = async {
//        while true do
//            let! (database, batch) = queue.Consume()
//            try
//                do! (readDocs database batch)
//            with | e ->
//                log.DebugWithException <| lazy("Exception on read",e)
//    }
//
//    let startConsumers = 
//        for i in [1..workerCount] do
//            let taskName = sprintf "Neo4j Read Queue Worker %d" i
//            runAsyncAsTask taskName cancellationToken consumer  |> ignore
//        ()
//    
//    member x.Work = queue.Work
