namespace Eventful.Tests

open Eventful
open System
open NUnit.Framework
open System.Threading.Tasks
open FsUnit

module GroupingBoundedQueueTests = 

    [<Test>]
    let ``Can enqueue and dequeue an item`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string>(1000)

        let groupName = "group"
        let itemValue = "item"

        let producer = 
            async {
                do! groupingQueue.AsyncAdd(groupName, itemValue)
            } |> Async.Start

        let consumer =
            async {
                let! (group, items) = groupingQueue.AsyncGet()
                group |> should equal groupName
                items |> Seq.head |> should equal itemValue
                items |> Seq.length |> should equal 1
            } |> Async.StartAsTask

        consumer.Wait ()

    [<Test>]
    let ``Consumer will wait for value`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string>(1000, 100000)

        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        let consumer =
            async {
                let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                do! groupingQueue.AsyncGet() |> Async.Ignore
                stopwatch.ElapsedMilliseconds |> should be (greaterThanOrEqualTo waitMilliseconds)
            } |> Async.StartAsTask

        async {
            do! Async.Sleep waitMilliseconds
            do! groupingQueue.AsyncAdd(groupName, itemValue)
        } |> Async.Start

        consumer.Wait ()

    [<Test>]
    let ``Producer will wait for consumers once queues are full`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string>(1, 1)

        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        async {
            do! Async.Sleep waitMilliseconds
            do! groupingQueue.AsyncGet() |> Async.Ignore
        } |> Async.Start

        let producer = 
            async {
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                stopwatch.ElapsedMilliseconds |> should be (greaterThanOrEqualTo waitMilliseconds)
            } |> Async.StartAsTask

        producer.Wait ()

    [<Test>]
    let ``Next items from the same group will not be consumed until previous items are complete`` () : unit =
        let groupingQueue = new GroupingBoundedQueue<string,string>(1000, 100000)

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
            do! Async.Sleep 10
            do! groupingQueue.AsyncAdd("group","item1")
        } |> Async.Start

        async {
            let! results = Async.Parallel [worker1; worker2]

            match results with
            | [|(s1,e1);(s2,e2)|] -> Assert.True(s1 < s2 && e1 < s2 || s2 < s1 && e2 < s1, "Consumers should not overlap")
            | x -> Assert.Fail(sprintf "Unexpected result: %A" x)
        } |> Async.RunSynchronously