namespace Eventful

open System
open System.Collections.Concurrent

type internal ParallelInOrderTransformerQueueItem<'TInput, 'TOutput> = {
    Index : int64
    Input : 'TInput
    OnComplete : ('TOutput -> unit)
}

type internal ParallelInOrderTransformerCompleteItem<'TOutput> = {
    Index : int64
    Output : 'TOutput
    OnComplete : ('TOutput -> unit)
}

type ParallelInOrderTransformer<'TInput,'TOutput>(work : 'TInput -> 'TOutput, ?maxItems : int, ?workerCount : int) =
    let log = createLogger <| sprintf "Eventful.ParallelInOrderTransformer<%s,%s>" typeof<'TInput>.Name typeof<'TOutput>.Name
    let currentIndex = ref -1L

    let maxItems = 
        match maxItems with
        | Some x -> x
        | None -> 100000

    let workerCount = 
        match workerCount with
        | Some x -> x
        | None -> 4

    let completeQueue = new BlockingCollection<ParallelInOrderTransformerCompleteItem<'TOutput>>(maxItems)

    let completeWorkerLoop () : unit =
        let pendingQueue = new System.Collections.Generic.SortedDictionary<int64,ParallelInOrderTransformerCompleteItem<'TOutput>>()

        let rec completeQueueItems nextIndex = 
            let (nextFound, nextValue) = pendingQueue.TryGetValue nextIndex
            if nextFound then
                nextValue.OnComplete nextValue.Output
                pendingQueue.Remove nextIndex |> ignore
                completeQueueItems (nextIndex + 1L)
            else
                nextIndex

        let nextIndex = ref 0L

        for item in completeQueue.GetConsumingEnumerable() do
            if item.Index = !nextIndex then
                item.OnComplete item.Output
                let newIndex = completeQueueItems (!nextIndex  + 1L)
                nextIndex := newIndex
            else
                pendingQueue.Add(item.Index, item)
        
    let queue = new BlockingCollection<ParallelInOrderTransformerQueueItem<'TInput, 'TOutput>>(maxItems)

    let workerLoop () : unit =
        for item in queue.GetConsumingEnumerable() do
            let output = work item.Input 
            completeQueue.Add
                {
                    ParallelInOrderTransformerCompleteItem.Index = item.Index
                    Output = output
                    OnComplete = item.OnComplete
                }

    let threads =
        List.init workerCount (fun _ -> createBackgroundThread workerLoop)

    do
        threads
        |> List.iter (fun t -> t.Start())

    do
        let completeQueueWorker = createBackgroundThread completeWorkerLoop
        completeQueueWorker.Start()

    member x.Process (input : 'TInput, onComplete : 'TOutput -> unit) = 
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
            completeQueue.CompleteAdding()
            queue.Dispose()