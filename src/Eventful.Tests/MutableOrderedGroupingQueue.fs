namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FSharpx.Collections

module MutableOrderedGroupingBoundedQueueTests = 
    [<Fact>]
    [<Trait("category", "foo3")>]
    let ``Can process single item`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new Eventful.CounterAgent()
        let rec consumer (counter : Eventful.CounterAgent)  = async {
            do! queue.Consume((fun (g, items) -> async {
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
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
    [<Trait("category", "foo5")>]
    let ``speed test for 1 million items to tracker`` () : unit =
        let maxValue  = 1000000L
        let items = [1L..maxValue]
        let rnd = new Random(1024)
        let randomItems = items |> Seq.sortBy (fun _ -> rnd.Next(1000000)) |> Seq.cache

        let tracker = new LastCompleteItemAgent2<int64>()

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
            do! queue.Consume((fun (g, items) -> async {
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