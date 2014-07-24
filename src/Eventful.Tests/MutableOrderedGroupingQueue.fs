namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FSharpx.Collections
open FsUnit.Xunit

module MutableOrderedGroupingBoundedQueueTests = 
    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can process single item`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new CounterAgent()
        let rec consumer (counter : CounterAgent)  = async {
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
    [<Trait("category", "unit")>]
    let ``Can process single item with 2 groups`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new CounterAgent()
        let rec consumer (counter : CounterAgent)  = async {
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
    [<Trait("category", "unit")>]
    let ``Fires Event When full`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>(10)

        let hasBeenFull = ref false

        queue.QueueFullEvent.Add(fun () -> hasBeenFull := true )

        async {
            for i in [1..100] do
                do! queue.Add(i, (fun _ -> Seq.singleton (i, i)))
        } |> Async.Start

        async {
            do! Async.Sleep(100)
            !hasBeenFull |> should equal true
        }   
        |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "performance")>]
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
    [<Trait("category", "performance")>]
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
    [<Trait("category", "unit")>]
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
            |> Seq.cache

        async {
            do! queueItems |> Async.Parallel |> Async.Ignore
            do! queue.CurrentItemsComplete()
            Assert.True((streamCount = store.Count), sprintf "store.Count %A should equal streamCount %A" store.Count streamCount)
            for pair in store do
                Assert.Equal(expectedValue, pair.Value)
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Test many items across many groups`` () : unit = 
        let myQueue = new MutableOrderedGroupingBoundedQueue<int, int>()

        let counter1 = new CounterAgent()
        let counter2 = new CounterAgent()
        let counter3 = new CounterAgent()
        let counter4 = new CounterAgent()

        let rec consumer (counter : CounterAgent)  = async {
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

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Producer will wait for consumers once queues are full`` () : unit = 
        let groupingQueue = new MutableOrderedGroupingBoundedQueue<string,string>(1)

        let groupName = "group"
        let itemValue = "item"

        let producer = 
            async {
                for _ in [1..10] do
                    do! groupingQueue.Add("itemValue",(fun _ -> Seq.singleton (groupName, itemValue)))
            } |> Async.StartAsTask


        // should not be able to queue all the items
        // the limit is not hard so we have a length of 1 but 10
        // items should be far too many 2 will probably get through
        producer.Wait (200) |> should equal false

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Next items from the same group will not be consumed until previous items are complete`` () : unit =
        let groupingQueue = new MutableOrderedGroupingBoundedQueue<string,string>(1000)

        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        let consumer (group, items) = async {
            let startTime = DateTime.Now
            do! Async.Sleep(100)
            let endTime = DateTime.Now
            return (startTime, endTime)
        }

        let worker = async {
                        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                        let result = ref (DateTime.MinValue,DateTime.MinValue)
                        let! work = groupingQueue.Consume((fun (g, items) -> async { 
                            let! r = consumer(g, items)
                            result := r }))
                        do! work
                        return (!result)
                      } 

        async {
            do! groupingQueue.Add("item1",(fun _ -> Seq.singleton ("item1", "group")))
            do! Async.Sleep 100 // needs to be long enough to ensure the first worker does not get a single item
            do! groupingQueue.Add("item1",(fun _ -> Seq.singleton ("item1", "group")))
        } |> Async.Start

        async {
            let! results = Async.Parallel [worker; worker]

            match results with
            | [|(s1,e1);(s2,e2)|] -> Assert.True(s1 < s2 && e1 <= s2 || s2 < s1 && e2 <= s1, sprintf "Consumers should not overlap (%A - %A) (%A - %A)" s1 e1 s2 e2)
            | x -> Assert.True(false,sprintf "Unexpected result: %A" x)
        } |> Async.RunSynchronously