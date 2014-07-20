namespace Eventful

type internal QueueItem<'T,'U> = 'T * AsyncReplyChannel<'U>
type internal BatchWork<'T,'U> = seq<QueueItem<'T,'U>>

type internal BatchingQueueMessage<'T,'U> =
    | Enqueue of QueueItem<'T,'U>
    | Consume of AsyncReplyChannel<BatchWork<'T,'U>>

type internal BatchingQueueState<'T,'U> = {
    Queue : System.Collections.Generic.Queue<QueueItem<'T,'U>>
}
with static member Zero = { Queue = new System.Collections.Generic.Queue<QueueItem<'T,'U>>(1000000) }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal BatchingQueueState =
    let enqeue (queue : BatchingQueueState<'T,'U>) (item : QueueItem<'T,'U>) =
        queue.Queue.Enqueue(item) 
        queue

    let getBatch (queue: BatchingQueueState<'T,'U>) size : (seq<QueueItem<'T,'U>> * BatchingQueueState<'T,'U>) =
        let batchSize = min size queue.Queue.Count

        let unfoldAcc size =
            if (size = 0) then
                None
            else 
                (Some (queue.Queue.Dequeue(), size - 1))

        let batch = 
            Seq.unfold unfoldAcc batchSize
            |> List.ofSeq
            |> Seq.ofList

        (batch, queue)

    let queueSize (queue: BatchingQueueState<'T,'U>) = 
        queue.Queue.Count

type BatchingQueue<'T,'U> 
    (
        maxBatchSize : int,
        maxQueueSize : int
    ) =

    let log = Common.Logging.LogManager.GetLogger("Eventful.BatchingQueue")

    let agent = newAgent "BatchingQueue" log (fun agent -> 
        let rec empty state = agent.Scan((fun msg -> 
            match msg with
            | Enqueue (item, reply) ->
                let state' = enqueue state item reply
                Some (next state')
            | Consume reply -> None)) 
        and hasWork state = async {
            let! msg = agent.Receive()
            match msg with
            | Enqueue (item, reply) ->
                let state' = enqueue state item reply
                return! (next state')
            | Consume reply -> 
                let state' = consume state reply
                return! (next state') }
        and full state = agent.Scan(fun msg ->
            match msg with
            | Consume reply ->
                let state' = consume state reply
                Some(next state')
            | _ -> None) 
        and enqueue state item reply = BatchingQueueState.enqeue state (item, reply)
        and consume state (reply :  AsyncReplyChannel<BatchWork<'T,'U>>) = 
            let (batch, state') = BatchingQueueState.getBatch state maxBatchSize
            reply.Reply batch
            state'
        and next state =
            let queueSize = state |> BatchingQueueState.queueSize
            if (queueSize = 0) then
                empty state
            elif (queueSize >= maxQueueSize) then
                full state
            else
                hasWork state 

        empty BatchingQueueState<'T,'U>.Zero
    )
    member x.Consume () : Async<BatchWork<'T,'U>> =
        agent.PostAndAsyncReply(fun ch -> Consume ch)

    member x.Work (item : 'T) : Async<'U> =
        agent.PostAndAsyncReply(fun ch -> Enqueue (item, ch))