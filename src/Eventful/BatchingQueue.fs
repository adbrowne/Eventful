namespace Eventful

type internal QueueItem<'T,'U> = 'T * AsyncReplyChannel<'U>
type internal BatchWork<'TKey, 'T,'U> = 'TKey * seq<QueueItem<'T,'U>>

type internal BatchingQueueMessage<'TKey, 'T,'U> =
    | Enqueue of 'TKey * QueueItem<'T,'U>
    | Consume of AsyncReplyChannel<BatchWork<'TKey, 'T,'U>>

type internal BatchingQueueState<'TKey,'T,'U when 'TKey : equality> = {
    Queues : System.Collections.Generic.Dictionary<'TKey,System.Collections.Generic.Queue<QueueItem<'T,'U>>>
    HasWork : System.Collections.Generic.HashSet<'TKey>
    mutable ItemCount : int
}
with static member Zero = 
        { 
            Queues = new System.Collections.Generic.Dictionary<'TKey,System.Collections.Generic.Queue<QueueItem<'T,'U>>>() 
            HasWork = new System.Collections.Generic.HashSet<'TKey>()
            ItemCount = 0
        }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal BatchingQueueState =
    let rnd = new System.Random()
    let enqeue (queue : BatchingQueueState<'TKey,'T,'U>) (key : 'TKey) (item : QueueItem<'T,'U>) =
        if (queue.Queues.ContainsKey key) then
            (queue.Queues.Item key).Enqueue item
        else
            let q = new System.Collections.Generic.Queue<QueueItem<'T,'U>>()
            q.Enqueue item
            queue.Queues.Add(key, q)
        queue.HasWork.Add key |> ignore
        queue.ItemCount <- queue.ItemCount + 1
        queue

    let getBatch (queue: BatchingQueueState<'TKey,'T,'U>) size : (BatchWork<'TKey, 'T,'U> * BatchingQueueState<'TKey,'T,'U>) =
        let key = queue.HasWork |> Seq.nth (rnd.Next(queue.HasWork.Count))
        let selectedQueue = queue.Queues.Item key
        let batchSize = min size selectedQueue.Count

        let unfoldAcc size =
            if (size = 0) then
                None
            else 
                (Some (selectedQueue.Dequeue(), size - 1))

        let batch = 
            Seq.unfold unfoldAcc batchSize
            |> List.ofSeq
            |> Seq.ofList

        if selectedQueue.Count = 0 then
            queue.HasWork.Remove key |> ignore
        else
            ()

        queue.ItemCount <- queue.ItemCount - batchSize
        ((key,batch), queue)

    let queueSize (queue: BatchingQueueState<'TKey,'T,'U>) = 
        queue.ItemCount

type BatchingQueue<'TKey,'T,'U when 'TKey : equality> 
    (
        maxBatchSize : int,
        maxQueueSize : int
    ) =

    let log = Common.Logging.LogManager.GetLogger("Eventful.BatchingQueue")

    let agent = newAgent "BatchingQueue" log (fun agent -> 
        let rec empty state = agent.Scan((fun msg -> 
            match msg with
            | Enqueue (key, (item, reply)) ->
                let state' = enqueue state key item reply
                Some (next state')
            | Consume reply -> None)) 
        and hasWork state = async {
            let! msg = agent.Receive()
            match msg with
            | Enqueue (key, (item, reply)) ->
                let state' = enqueue state key item reply
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
        and enqueue state key item reply = BatchingQueueState.enqeue state key (item, reply)
        and consume state (reply :  AsyncReplyChannel<BatchWork<'TKey,'T,'U>>) = 
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

        empty BatchingQueueState<'TKey,'T,'U>.Zero
    )

    member x.Consume () : Async<BatchWork<'TKey, 'T,'U>> =
        agent.PostAndAsyncReply(fun ch -> Consume ch)

    member x.Work (key : 'TKey) (item : 'T) : Async<'U> =
        agent.PostAndAsyncReply(fun ch -> Enqueue (key, (item, ch)))