namespace Eventful

open System
open System.Collections.Concurrent

type internal ParallelInOrderTransformerQueueItem<'TInput, 'TOutput> = {
    Index : int64
    Input : 'TInput
    OnComplete : ('TOutput -> Async<unit>)
}

type internal ParallelInOrderTransformerCompleteItem<'TOutput> = {
    Index : int64
    Output : 'TOutput
    OnComplete : ('TOutput -> Async<unit>)
}


type ParallelInOrderTransformer<'TInput,'TOutput>(work : 'TInput -> Async<'TOutput>, ?maxItems : int, ?workerCount : int) =
    let log = createLogger <| sprintf "Eventful.ParallelInOrderTransformer<%s,%s>" typeof<'TInput>.Name typeof<'TOutput>.Name
    let currentIndex = ref -1L

    let maxItems = 
        match maxItems with
        | Some x -> x
        | None -> 100

    let workerCount = 
        match workerCount with
        | Some x -> x
        | None -> 10

    let runAgent (agent : Agent<_>) = 
        let completeQueue = new System.Collections.Generic.SortedDictionary<int64,ParallelInOrderTransformerCompleteItem<'TOutput>>()

        let rec completeQueueItems nextIndex = async {
            let (nextFound, nextValue) = completeQueue.TryGetValue nextIndex
            if nextFound then
                do! nextValue.OnComplete nextValue.Output
                completeQueue.Remove nextIndex |> ignore
                return! completeQueueItems (nextIndex + 1L)
            else
                return nextIndex
        }

        let rec loop nextIndex = async {
            let! (item : ParallelInOrderTransformerCompleteItem<'TOutput>) = agent.Receive()
            if item.Index = nextIndex then
                do! item.OnComplete item.Output
                let! nextIndex = completeQueueItems (nextIndex + 1L)
                return! loop nextIndex
            else
                completeQueue.Add(item.Index, item)
                return! loop nextIndex
        }
        loop 0L

    let orderingAgent = newAgent "NewParallelTransformer" log runAgent

    let queue = new BlockingCollection<ParallelInOrderTransformerQueueItem<'TInput, 'TOutput>>(maxItems)

    let workerLoop (o : obj) : unit =
        for item in queue.GetConsumingEnumerable() do
            async {
                let! output = work item.Input 
                orderingAgent.Post 
                    {
                        ParallelInOrderTransformerCompleteItem.Index = item.Index
                        Output = output
                        OnComplete = item.OnComplete
                    }
            } |> Async.RunSynchronously

    let threads = 
        [1..workerCount]
        |> List.map (fun _ -> new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(workerLoop)))

    do
        threads
        |> List.iter (fun t -> t.Start())

    member x.Process (input : 'TInput, onComplete : 'TOutput -> Async<unit>) = 
        let index = System.Threading.Interlocked.Increment currentIndex
        queue.Add 
            { Index = index
              Input = input
              OnComplete = onComplete }

    interface IDisposable with
        member x.Dispose () = 
            // will make GetConsumingEnumerable finish
            // which means worker threads stop
            queue.CompleteAdding() 
            queue.Dispose()