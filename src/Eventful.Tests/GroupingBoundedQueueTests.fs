namespace Eventful.Tests

open Eventful
open NUnit.Framework
open System.Threading.Tasks
open FsUnit

module GroupingBoundedQueueTests = 

    [<Test>]
    let ``Can enqueue and dequeue an item`` () : unit = 
        let tcs = new TaskCompletionSource<bool>()

        let groupName = "group"
        let itemValue = "item"

        let consumer group items =
            async {
                group |> should equal groupName
                items |> Seq.head |> should equal itemValue
                items |> Seq.length |> should equal 1
                tcs.SetResult true
            }

        let groupingQueue = new GroupingBoundedQueue<string,string>(1000, 100000, 1, consumer)

        let producer = 
            async {
                do! groupingQueue.AsyncAdd(groupName, itemValue)
            } |> Async.Start


        tcs.Task.Wait()

    [<Test>]
    let ``Consumer will wait for value`` () : unit = 
        let groupName = "group"
        let itemValue = "item"

        let tcs = new TaskCompletionSource<bool>()
        let waitMilliseconds = 200

        let consumer _ _ =
            async {
                tcs.SetResult(true)
            }

        let groupingQueue = new GroupingBoundedQueue<string,string>(1000, 100000, 1, consumer)

        let stopwatch = System.Diagnostics.Stopwatch.StartNew()

        async {
            do! Async.Sleep waitMilliseconds
            do! groupingQueue.AsyncAdd(groupName, itemValue)
        } |> Async.Start

        tcs.Task.Wait ()
        stopwatch.ElapsedMilliseconds |> should be (greaterThanOrEqualTo waitMilliseconds)

    [<Test>]
    let ``Producer will wait for consumers once queues are full`` () : unit = 
        let groupName = "group"
        let itemValue = "item"

        let waitMilliseconds = 200

        let consumer _ _ =
            async {
                do! Async.Sleep waitMilliseconds
            }

        let groupingQueue = new GroupingBoundedQueue<string,string>(1, 1, 1, consumer)

        let producer = 
            async {
                // will be assigned to the single consumer
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                // will be enqueued almost immediately
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                let stopwatch = System.Diagnostics.Stopwatch.StartNew()
                // will need to wait for the first sleep so the second item can be dequeued
                do! groupingQueue.AsyncAdd(groupName, itemValue)
                stopwatch.ElapsedMilliseconds |> should be (greaterThanOrEqualTo waitMilliseconds)
            } |> Async.StartAsTask

        producer.Wait ()