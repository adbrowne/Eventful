namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FSharpx.Collections
open FsUnit.Xunit

module MutableOrderedGroupingBoundedQueueTests = 
    [<Fact>]
    [<Trait("category", "foo3")>]
    let ``Can process single item`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new Eventful.CounterAgent()
        let rec consumer (counter : Eventful.CounterAgent)  = async {
            let! work =  queue.Consume((fun (g, items) -> async {
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
            do! work
            return! consumer counter
        }

        consumer counter |> Async.StartAsTask |> ignore

        async {
            do! queue.Add(1, (fun _ -> Seq.singleton(1, 1)))
            do! queue.CurrentItemsComplete()
            let! result = counter.Get()
            Assert.Equal(1, result); 
        } |> Async.RunSynchronously

    open System.Collections.Generic

    [<Fact>]
    let ``Can process single item with 2 groups`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new Eventful.CounterAgent()
        let rec consumer (counter : Eventful.CounterAgent)  = async {
            let! work = queue.Consume((fun (g, items) -> async {
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
            do! work
            return! consumer counter
        }

        async {
            do! queue.Add(1, (fun _ -> [(1,1);(1,2)] |> List.toSeq))
            do! queue.CurrentItemsComplete()
            let! result = counter.Get()
            result |> should equal 2
        } |> Async.Start


    [<Fact>]
    [<Trait("category", "foo5")>]
    let ``speed test for 1 million items to tracker`` () : unit =
        let maxValue  = 1000000L
        let items = [1L..maxValue]
        let rnd = new Random(1024)
        let randomItems = items |> Seq.sortBy (fun _ -> rnd.Next(1000000)) |> Seq.cache

        let tracker = new LastCompleteItemAgent<int64>()

        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        async {
            for item in items do
                do! tracker.Start(item)

            for item in randomItems do
                tracker.Complete(item)
            
            tracker.NotifyWhenComplete (maxValue, None, async { tcs.SetResult true })
        } |> Async.RunSynchronously

        tcs.Task.Wait()

    [<Fact>]
    [<Trait("category", "foo5")>]
    let ``speed test for 1 million items to empty agent`` () : unit =
        let maxValue  = 1000000L
        let items = [1L..maxValue]
        let rnd = new Random(1024)
        let randomItems = items |> Seq.sortBy (fun _ -> rnd.Next(1000000)) |> Seq.cache

        let agent = Agent.Start(fun agent -> 
            let rec loop () = async {
                let! (msg : AsyncReplyChannel<unit>) = agent.Receive()
                msg.Reply() 
                return! loop ()}
            loop ()) 

        async {
            for item in items do
                do! agent.PostAndAsyncReply(fun ch -> ch)

            for item in randomItems do
                do! agent.PostAndAsyncReply(fun ch -> ch)
            
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "foo3")>]
    let ``Can calculate correct values`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<Guid, int>()
        let store = new System.Collections.Generic.Dictionary<Guid, int>()
        let monitor = new Object()
        let streamCount = 100
        let itemCount = 100
        let inputItems = TestEventStream.sequentialValues streamCount itemCount |> Seq.cache

        let accumulator s a =
            if(a % 2 = 0) then
                s + a
            else
                s - a

        let rec consumer ()  = async {
            let! work = queue.Consume((fun (g, items) -> async {
                let current = 
                    if store.ContainsKey g then
                        store.Item(g)
                    else 
                        0
                let result = 
                    items |> Seq.fold accumulator current
                   
                lock monitor (fun () -> 
                    store.Remove g |> ignore

                    store.Add(g, result)
                    ()
                )
                return ()
            }))
            do! work
            return! consumer ()
        }

        consumer () |> Async.StartAsTask |> ignore
        consumer () |> Async.StartAsTask |> ignore
        consumer () |> Async.StartAsTask |> ignore
        consumer () |> Async.StartAsTask |> ignore
        consumer () |> Async.StartAsTask |> ignore
        consumer () |> Async.StartAsTask |> ignore
        consumer () |> Async.StartAsTask |> ignore

        let expectedValue = [1..itemCount] |> List.fold accumulator 0

        let queueItems = 
            inputItems 
            |> Seq.map (fun (eventPosition, key, value) -> queue.Add(value, (fun v -> Seq.singleton (value, key)))) 
        async {
            do! queueItems |> Async.Parallel |> Async.Ignore
            do! queue.CurrentItemsComplete()
            Assert.True((streamCount = store.Count), sprintf "store.Count %A should equal streamCount %A" store.Count streamCount)
            for pair in store do
                Assert.Equal(expectedValue, pair.Value)
        } |> Async.RunSynchronously

    [<Fact>]
    let ``Test many items across many groups`` () : unit = 
        let myQueue = new MutableOrderedGroupingBoundedQueue<int, int>()

        let counter1 = new Eventful.CounterAgent()
        let counter2 = new Eventful.CounterAgent()
        let counter3 = new Eventful.CounterAgent()
        let counter4 = new Eventful.CounterAgent()

        let rec consumer (counter : Eventful.CounterAgent)  = async {
            let! work = myQueue.Consume((fun (g, items) -> async {
                // Console.WriteLine(sprintf "Group: %A Items: %A ItemCount: %d" g items (items |> Seq.length))
                // do! Async.Sleep 100
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
            do! work
            return! consumer counter
        }

        consumer counter1 |> Async.Start
        consumer counter2 |> Async.Start
        consumer counter3 |> Async.Start
        consumer counter1 |> Async.Start
        consumer counter2 |> Async.Start
        consumer counter3 |> Async.Start
        consumer counter1 |> Async.Start
        consumer counter2 |> Async.Start
        consumer counter3 |> Async.Start
        consumer counter1 |> Async.Start
        consumer counter2 |> Async.Start
        consumer counter3 |> Async.Start
        consumer counter4 |> Async.Start
        consumer counter4 |> Async.Start
        consumer counter4 |> Async.Start
        consumer counter4 |> Async.Start
        consumer counter4 |> Async.Start
        consumer counter4 |> Async.Start

        async {

            for i in [1..100000] do
                do! myQueue.Add(i, (fun input -> Seq.singleton (input, input)))

            do! myQueue.CurrentItemsComplete()

            let! value1 = counter1.Get()
            let! value2 = counter2.Get()
            let! value3 = counter3.Get()
            let! value4 = counter4.Get()
           
            printfn "Received %d %d %d %d total: %d" value1 value2 value3 value4 (value1 + value2 + value3 + value4) 

        } |> Async.RunSynchronously