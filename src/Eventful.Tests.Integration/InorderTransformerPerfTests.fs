namespace Eventful.Tests.Integration

open Xunit
open System
open Eventful
open Eventful.EventStore
open System.Threading.Tasks
open EventStore.ClientAPI
open FSharpx

module EventStorePlay =
    let itemCount = 100000
    
    let eventCount = ref 0
    let result = ref 0

    let runTest eventHandler (completeTask : Task<bool>) =
        let stopwatch = System.Diagnostics.Stopwatch.StartNew()

        eventCount := 0

        for i in [1..itemCount] do
            eventHandler i |> Async.RunSynchronously

        completeTask.Wait()

        stopwatch.Stop()

        printfn "%d events in %f seconds %f" !eventCount stopwatch.Elapsed.TotalSeconds (float !eventCount / stopwatch.Elapsed.TotalSeconds)
        ()
        
    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Direct incriment`` () = 
        let eventHandler i =
            async { 
                eventCount := !eventCount + 1 
                if !eventCount <> i then
                    printfn "Value out of order %d %d" !eventCount i
            }

        runTest eventHandler (Task.FromResult(true))

    let noWork x = async {
        return x
    }

    let onComplete (tcs : TaskCompletionSource<bool>) i =
        let newValue = System.Threading.Interlocked.Increment eventCount
        if i <> newValue then
            printfn "Value out of order %d %d" newValue i
            
        if newValue= itemCount - 1 then
            tcs.SetResult true
        async.Zero()
            
    type QueueItem<'TInput, 'TOutput> = {
        Index : int64
        Input : 'TInput
        OnComplete : ('TOutput -> Async<unit>)
    }

    type CompleteItem<'TOutput> = {
        Index : int64
        Output : 'TOutput
        OnComplete : ('TOutput -> Async<unit>)
    }

    type NewParallelTransformer<'TInput, 'TOutput>(work : 'TInput -> Async<'TOutput>) =
        let workerCount = 20
        let maxQueueSize = 1000
        let currentIndex = ref -1L

        let log = createLogger "NewParallelTransformer"

        let runAgent (agent : Agent<_>) = 
            let completeQueue = new System.Collections.Generic.SortedDictionary<int64,CompleteItem<'TOutput>>()

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
                let! (item : CompleteItem<'TOutput>) = agent.Receive()
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

        let queue = new System.Collections.Concurrent.BlockingCollection<QueueItem<'TInput, 'TOutput>>(maxQueueSize)

        let workerLoop (o : obj) : unit =
            for item in queue.GetConsumingEnumerable() do
                async {
                    let! output = work item.Input 
                    orderingAgent.Post 
                        {
                            CompleteItem.Index = item.Index
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

        member x.Process (input : 'TInput) (onComplete : 'TOutput -> Async<unit>) = 
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

    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Run in parallel NEW code no work`` () = 

        use transformer = new NewParallelTransformer<int,int>(noWork)

        let tcs = new TaskCompletionSource<bool>()
        let eventHandler item =
            transformer.Process item (onComplete tcs)
            async.Zero()

        let getEventCount () = !eventCount

        runTest eventHandler tcs.Task

    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Run in parallel old code no work`` () = 

        let work x = async {
            return x
        }

        let transformer = new Eventful.ParallelInOrderTransformer<int,int>(noWork)

        let tcs = new TaskCompletionSource<bool>()
        let eventHandler item =
            transformer.Process (item, onComplete tcs)
            async.Zero()

        runTest eventHandler tcs.Task

    let realWork x = async {
        let sum = [1L..10000L] |> Seq.sum
        return x
    }

    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Run in parallel NEW code with work`` () = 

        use transformer = new NewParallelTransformer<int,int>(realWork)

        let tcs = new TaskCompletionSource<bool>()
        let eventHandler item =
            transformer.Process item (onComplete tcs)
            async.Zero()

        let getEventCount () = !eventCount

        runTest eventHandler tcs.Task


    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Run in parallel old code with work`` () = 

        let transformer = new Eventful.ParallelInOrderTransformer<int,int>(realWork)
        let tcs = new TaskCompletionSource<bool>()

        let eventHandler item =
            transformer.Process (item, onComplete tcs)
            async.Zero()

        runTest eventHandler  tcs.Task