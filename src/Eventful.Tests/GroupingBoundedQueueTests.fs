namespace Eventful.Tests

open Eventful
open NUnit.Framework
open System.Threading.Tasks
open FsUnit

module GroupingBoundedQueueTests = 

    [<Test>]
    let ``Can enqueue and dequeue an item`` () : unit = 
        let groupingQueue = new GroupingBoundedQueue<string,string>(1000, 100000)

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