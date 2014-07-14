namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit

type ParallelInOrderTransformerTests<'TInput, 'TOutput> =
    | EnqueueMsg of 'TInput * ('TOutput -> Async<unit>) * AsyncReplyChannel<unit>
    | CompleteMsg of int64 * 'TOutput

type ParallelTransformerItemState<'TInput, 'TOutput> = 
    | Queued of int64 * 'TInput * ('TOutput -> Async<unit>)
    | Working of int64 * 'TInput * ('TOutput -> Async<unit>)
    | Complete of int64 * 'TOutput * ('TOutput -> Async<unit>)

type ParallelTransformerQueue<'TInput, 'TOutput> = 
    System.Collections.Generic.SortedDictionary<int64, ParallelTransformerItemState<'TInput, 'TOutput>>

type ParallelInOrderTransformerState<'TInput,'TOutput> = {
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

type ParallelInOrderTransformer<'TInput,'TOutput>(work : 'TInput -> Async<'TOutput>,?maxItems : int, ?workers : int) =
    let log = Common.Logging.LogManager.GetLogger("Eventful.ParallelInOrderTransformer")

    let maxItems = 
        match maxItems with
        | Some x -> x
        | None -> 100

    let workers = 
        match workers with
        | Some x -> x
        | None -> 10

    let newAgent (log : Common.Logging.ILog) f  =
        let agent= Agent.Start(f)
        agent.Error.Add(fun e -> log.Error("Exception thrown by ParallelInOrderTransformer", e))
        agent

    let rec clearCompleteItemsFromHead (queue : ParallelTransformerQueue<'TInput, 'TOutput>) =
        if queue.Count > 0 then
            let head = queue.Keys |> Seq.head
            match queue.Item(head) with
            | Complete (_, output, continuation) ->
                continuation output |> Async.RunSynchronously
                queue.Remove head |> ignore
                clearCompleteItemsFromHead queue
            | _ ->
                ()

    let transformerAgent = newAgent log (fun agent ->
        let rec loop (state : ParallelInOrderTransformerState<'TInput,'TOutput>) = 
            agent.Scan(fun msg -> 
            match msg with
            | EnqueueMsg (input, continuation, reply) -> 
                if state.Queue.Count < maxItems then 
                    reply.Reply()
                    let state' = 
                        if state.RunningItems < workers then
                            async { 
                                let! output = work input
                                agent.Post <| CompleteMsg (state.NextIndex, output) 
                            } |> Async.StartAsTask |> ignore

                            state.Queue.Add(state.NextIndex, Working(state.NextIndex, input, continuation))
                            { state with 
                                RunningItems = state.RunningItems + 1; 
                                NextIndex = state.NextIndex + 1L }
                        else
                            state.Queue.Add(state.NextIndex, Queued(state.NextIndex, input, continuation))
                            { state with 
                                RunningItems = state.RunningItems + 1; 
                                NextIndex = state.NextIndex + 1L }
                    Some (async { return! (loop state') })
                else
                    None
            | CompleteMsg (index, output) -> 
                let queueEntry = 
                    match state.Queue.Item(index) with
                    | Working (_, _, continuation) ->
                        Complete(index, output, continuation)
                    | _ -> failwith "Got complete entry for item not in working state"
                state.Queue.Remove index |> ignore
                state.Queue.Add(index, queueEntry)

                clearCompleteItemsFromHead state.Queue

                Some (loop { state with RunningItems = state.RunningItems - 1 })
        )

        loop ParallelInOrderTransformerState<'TInput, 'TOutput>.Zero)

    member x.Process(input: 'TInput, onComplete : 'TOutput -> Async<unit>) : unit =
        transformerAgent.PostAndAsyncReply(fun ch -> EnqueueMsg(input, onComplete, ch)) |> Async.RunSynchronously

module ParallelInOrderTransformerTests = 

    [<Fact>]
    let ``Transformer does not reorder operations`` () : unit = 
        let monitor = new obj()

        let received = ref List.empty

        let callback i = async {
            lock monitor (fun () -> 
                received := (i::(!received ))
            )
        }

        let transformer = new ParallelInOrderTransformer<int,int>((fun i -> async { return i }), 100, 100)

        for i in [1..100] do
            transformer.Process(i, callback)

        Async.Sleep(1000) |> Async.RunSynchronously

        !received |> List.rev |> should equal ([1..100])