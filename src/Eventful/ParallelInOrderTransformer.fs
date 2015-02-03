namespace Eventful

open System
open System.Threading
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

type ProducerConsumerQueue<'T> (maxItems: int, workerCount : int, workerLoop) =
    let workerShutDownEvent = new CountdownEvent(workerCount)
    let queue = new BlockingCollection<'T>(maxItems)

    let threadWorker () =
        try
            workerLoop queue
        finally
            workerShutDownEvent.Signal() |> ignore

    do
        List.init workerCount (fun _ -> createBackgroundThread threadWorker)
        |> List.iter (fun t -> t.Start())

    member x.Add (item : 'T) =
        queue.Add(item)

    member x.Dispose () = 
        // will make GetConsumingEnumerable finish
        // which means worker thread should stop
        queue.CompleteAdding() 

        // wait for all the worker threads to stop
        workerShutDownEvent.Wait()

        // dispose the queue
        queue.Dispose()

    interface IDisposable with
        member x.Dispose () = x.Dispose()
    
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

    let completeWorkerLoop (completeQueue : BlockingCollection<ParallelInOrderTransformerCompleteItem<'TOutput>>) : unit =
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
        
    let completeQueue = new ProducerConsumerQueue<_>(maxItems, 1, completeWorkerLoop)

    let workerLoop (queue: BlockingCollection<ParallelInOrderTransformerQueueItem<'TInput, 'TOutput>>) : unit =
        for item in queue.GetConsumingEnumerable() do
            let output = work item.Input 
            completeQueue.Add
                {
                    ParallelInOrderTransformerCompleteItem.Index = item.Index
                    Output = output
                    OnComplete = item.OnComplete
                }
    let workQueue = new ProducerConsumerQueue<_>(maxItems, workerCount, workerLoop)

    member x.Process (input : 'TInput, onComplete : 'TOutput -> unit) = 
        let index = System.Threading.Interlocked.Increment currentIndex
        workQueue.Add 
            { Index = index
              Input = input
              OnComplete = onComplete }

    interface IDisposable with
        member x.Dispose () = 
            workQueue.Dispose()
            completeQueue.Dispose()