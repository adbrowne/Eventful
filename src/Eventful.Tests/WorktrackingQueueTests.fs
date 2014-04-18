namespace Eventful.Tests

open Eventful
open NUnit.Framework
open System.Threading.Tasks
open FsUnit

module WorktrackingQueueTests = 

    [<Test>]
    let ``Completion function is called when item complete`` () : unit =
        let groupingFunction = Set.singleton << fst

        let tcs = new TaskCompletionSource<bool>()

        let completedItem = ref ("blank", "blank")
        let complete item = async {
            do! Async.Sleep(100)
            completedItem := item
            tcs.SetResult true
        }

        let worktrackingQueue = new WorktrackingQueue<string,(string * string)>(100000, groupingFunction, complete, 10, (fun _ _ -> Async.Sleep(100)))
        worktrackingQueue.Add ("group", "item") |> Async.Start

        tcs.Task.Wait()

        !completedItem |> fst |> should equal "group"
        !completedItem |> snd |> should equal "item"

    [<Test>]
    let ``Can run multiple items`` () : unit =
        let groupingFunction = Set.singleton << fst

        let completedItem = ref ("blank", "blank")
        let complete item = async {
            do! Async.Sleep(100)
            completedItem := item
        }

        let work (group:string) items = async {
                System.Console.WriteLine("Work item {0}", group)
            }

        let worktrackingQueue = new WorktrackingQueue<string,(string * string)>(100000, groupingFunction, complete, 10, work)
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously
        worktrackingQueue.Add ("group", "item") |> Async.RunSynchronously

        worktrackingQueue.AsyncComplete () |> Async.RunSynchronously

    [<Test>]
    let ``Given item split into 2 groups When complete Then Completion function is only called once`` () : unit =
        let groupingFunction _ = [1;2] |> Set.ofList

        let completeCount = new CounterAgent()

        let complete item = async {
            // do! Async.Sleep(100)
            do! completeCount.Incriment ()
        }

        async {
            let worktrackingQueue = new WorktrackingQueue<int,(string * string)>(100000, groupingFunction, complete, 10, (fun _ _ -> Async.Sleep(100)))
            do! worktrackingQueue.Add ("group", "item")
            do! worktrackingQueue.AsyncComplete()
            do! Async.Sleep 100
            let! count = completeCount.Get()
            count |> should equal 1
        } |> Async.RunSynchronously

    [<Test>]
    let ``Given empty queue When complete Then returns immediately`` () : unit =
        let worktrackingQueue = new WorktrackingQueue<unit,string>(100000, (fun _ -> Set.singleton ()), (fun _ -> Async.Sleep(1)), 10, (fun _ _ -> Async.Sleep(1)))
        worktrackingQueue.AsyncComplete() |> Async.RunSynchronously
