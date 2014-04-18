namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit

module GroupingBoundedQueueTests = 

    [<Fact>]
    let ``Can enqueue and dequeue an item`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string, (string * list<string>)>(1000)

        let groupName = "group"
        let itemValue = "item"

        let producer = 
            async {
                do! groupingQueue.AsyncAdd(groupName, itemValue)
            } |> Async.Start

        let consumer =
            async {
                let! (group, items) = groupingQueue.AsyncConsume((fun x -> async { return x }))
                group |> should equal groupName
                items |> Seq.head |> should equal itemValue
                items |> Seq.length |> should equal 1
            } |> Async.StartAsTask

        consumer.Wait ()

    [<Fact>]
    let ``Can run multiple items`` () : unit =
        let groupingQueue = new GroupingBoundedQueue<string, int, (string * list<int>)>(1000)

        let groupName = "group"
        let itemValue = "item"

        let producer = 
            async {
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
                do! Async.Sleep 100
                do! groupingQueue.AsyncAdd(groupName, 1)
            } |> Async.Start

        let total = ref 0

        let doWork (group, items) = async {
                let sum = items |> List.sum            
                let current = System.Threading.Interlocked.Add(total, sum)
                Console.WriteLine("Current: {0}", current)
                return (group, items)
            }
            
        let consumer =
            async {
                while !total < 8 do
                    Console.WriteLine("Current total: {0}", !total)
                    do! groupingQueue.AsyncConsume(doWork) |> Async.Ignore
                    do! Async.Sleep 100
            } |> Async.StartAsTask

        consumer.Wait ()

    [<Fact>]
    let ``Consumer will wait for value`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string, unit>(1000)

        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        let consumer =
            async {
                let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                do! groupingQueue.AsyncConsume((fun _ -> async { return () })) |> Async.Ignore
                stopwatch.ElapsedMilliseconds |> should be (greaterThanOrEqualTo <| int64 (waitMilliseconds - 10))
            } |> Async.StartAsTask

        async {
            do! Async.Sleep waitMilliseconds
            do! groupingQueue.AsyncAdd(groupName, itemValue)
        } |> Async.Start

        consumer.Wait ()

    [<Fact>]
    let ``Producer will wait for consumers once queues are full`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string, unit>(1)

        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        let producer = 
            async {
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                stopwatch.ElapsedMilliseconds |> should be (greaterThanOrEqualTo <| int64 waitMilliseconds)
            } |> Async.StartAsTask

        async {
            do! Async.Sleep waitMilliseconds
            do! groupingQueue.AsyncConsume((fun _ -> async { return () })) |> Async.Ignore
        } |> Async.Start

        producer.Wait ()

    [<Fact>]
    let ``Next items from the same group will not be consumed until previous items are complete`` () : unit =
        let groupingQueue = new GroupingBoundedQueue<string,string, (DateTime * DateTime)>(1000)

        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        let consumer (group, items) = async {
            let startTime = DateTime.Now
            do! Async.Sleep(100)
            let endTime = DateTime.Now
            return (startTime, endTime)
        }

        let worker1 = async {
                        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                        let! result = groupingQueue.AsyncConsume(consumer)
                        return result
                      } 

        let worker2 = async {
                        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                        let! result = groupingQueue.AsyncConsume(consumer)
                        return result
                      } 

        async {
            do! groupingQueue.AsyncAdd("group","item1")
            do! Async.Sleep 100 // needs to be long enough to ensure the first worker does not get a single item
            do! groupingQueue.AsyncAdd("group","item1")
        } |> Async.Start

        async {
            let! results = Async.Parallel [worker1; worker2]

            match results with
            | [|(s1,e1);(s2,e2)|] -> Assert.True(s1 < s2 && e1 <= s2 || s2 < s1 && e2 <= s1, sprintf "Consumers should not overlap (%A - %A) (%A - %A)" s1 e1 s2 e2)
            | x -> Assert.True(false,sprintf "Unexpected result: %A" x)
        } |> Async.RunSynchronously