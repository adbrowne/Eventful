namespace Eventful.Raven

open Eventful
open System
open Metrics

type RavenReadQueue 
    (
        documentStore:Raven.Client.IDocumentStore, 
        maxBatchSize : int,
        maxQueueSize : int,
        workerCount : int,
        cache : System.Runtime.Caching.MemoryCache
    ) =

    let readDocs databaseName (docs : seq<(seq<GetDocRequest> * AsyncReplyChannel<seq<GetDocResponse>>)>) : Async<unit>  = 

        let request = 
            docs
            |> Seq.collect (fun i -> fst i) 
           
        async {
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

            return ()
        }
        

    let queue = new BatchingQueue<string, seq<string * Type>, seq<GetDocResponse>>(maxBatchSize, maxQueueSize)

    let consumer = async {
        while true do
            let! (database, batch) = queue.Consume()
            do! (readDocs database batch)
    }

    let blah = 
        for _ in [1..workerCount] do
            consumer |> Async.StartAsTask |> ignore
        ()
    
    member x.Work = queue.Work