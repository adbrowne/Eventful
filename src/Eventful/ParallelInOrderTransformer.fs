namespace Eventful

open FSharpx

type internal ParallelInOrderTransformerTests<'TInput, 'TOutput> =
    | EnqueueMsg of 'TInput * ('TOutput -> Async<unit>) * AsyncReplyChannel<unit>
    | CompleteMsg of int64 * 'TOutput

type internal ParallelTransformerItemState<'TInput, 'TOutput> = 
    | Queued of int64 * 'TInput * ('TOutput -> Async<unit>)
    | Working of int64 * 'TInput * ('TOutput -> Async<unit>)
    | Complete of int64 * 'TOutput * ('TOutput -> Async<unit>)

type internal ParallelTransformerQueue<'TInput, 'TOutput> = 
    System.Collections.Generic.SortedDictionary<int64, ParallelTransformerItemState<'TInput, 'TOutput>>

type internal ParallelInOrderTransformerState<'TInput,'TOutput> = {
    RunningItems : int
    NextIndex : int64
    Queue : ParallelTransformerQueue<'TInput, 'TOutput>
}
with 
    static member Zero = 
        { 
            RunningItems = 0; 
            NextIndex = 0L; 
            Queue = new ParallelTransformerQueue<'TInput, 'TOutput>() 
        }

type ParallelInOrderTransformer<'TInput,'TOutput>(work : 'TInput -> Async<'TOutput>, ?maxItems : int, ?workers : int) =
    let log = Common.Logging.LogManager.GetLogger("Eventful.ParallelInOrderTransformer")

    let maxItems = 
        match maxItems with
        | Some x -> x
        | None -> 100

    let workers = 
        match workers with
        | Some x -> x
        | None -> 10

    let rec clearCompleteItemsFromHead (queue : ParallelTransformerQueue<'TInput, 'TOutput>) =
        if queue.Count > 0 then
            let headIndex = queue.Keys |> Seq.head
            let headQueueEntry = queue.Item(headIndex)
            match headQueueEntry with
            | Complete (_, output, continuation) ->
                continuation output |> Async.RunSynchronously
                queue.Remove headIndex |> ignore
                clearCompleteItemsFromHead queue
            | _ ->
                ()

    let transformerAgent = newAgent "ParallelInOrderTransformer" log (fun agent ->
        let runItem (index : int64) input = 
            async { 
                let! output = work input
                agent.Post <| CompleteMsg (index, output) 
            } |> Async.StartAsTask |> ignore

        let rec loop (state : ParallelInOrderTransformerState<'TInput,'TOutput>) = 
            agent.Scan(fun msg -> 
            match msg with
            | EnqueueMsg (input, continuation, reply) -> 
                if state.Queue.Count < maxItems then 
                    reply.Reply()
                    let state' = 
                        if state.RunningItems < workers then
                            runItem state.NextIndex input
                            state.Queue.Add(state.NextIndex, Working(state.NextIndex, input, continuation))
                            { state with 
                                RunningItems = state.RunningItems + 1; 
                                NextIndex = state.NextIndex + 1L }
                        else
                            state.Queue.Add(state.NextIndex, Queued(state.NextIndex, input, continuation))
                            { state with 
                                NextIndex = state.NextIndex + 1L }
                    Some (async { return! (loop state') })
                else
                    None
            | CompleteMsg (index : int64, output) -> 
                // update the queue entry
                let queueEntry = 
                    match state.Queue.Item(index) with
                    | Working (_, _, continuation) ->
                        Complete(index, output, continuation)
                    | _ -> failwith "Got complete entry for item not in working state"

                state.Queue.Remove index |> ignore
                state.Queue.Add(index, queueEntry)

                // run any waiting continuations (including maybe this one)
                clearCompleteItemsFromHead state.Queue

                // start a new item if one is waiting
                state.Queue.Values
                |> Seq.collect (function
                    | Queued (index, input, continuation) -> Seq.singleton (index, input, continuation)
                    | _ -> Seq.empty)
                |> FSharpx.Collections.Seq.tryHead
                |> function
                    | Some (index, input, continuation) ->
                       runItem index input
                       state.Queue.Remove(index) |> ignore
                       state.Queue.Add(index, Working(index, input, continuation)) 
                       Some (loop state)
                    | None ->
                       Some (loop { state with RunningItems = state.RunningItems - 1 })
        )

        loop ParallelInOrderTransformerState<'TInput, 'TOutput>.Zero)

    member x.Process(input: 'TInput, onComplete : 'TOutput -> Async<unit>) : unit =
        transformerAgent.PostAndAsyncReply(fun ch -> EnqueueMsg(input, onComplete, ch)) |> Async.RunSynchronously