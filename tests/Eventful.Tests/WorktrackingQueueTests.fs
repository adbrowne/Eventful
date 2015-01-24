﻿namespace Eventful.Tests

open Eventful
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open System

module WorktrackingQueueTests = 

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Grouper throwing exception does not prevent future events`` () : unit = 
        let monitor = new System.Object()
        let itemsReceived = ref Set.empty

        let groupingFunction x = 
            if x = 0 then
                failwith "0 is bad"
            else 
                Seq.singleton (x, x)

        let work = (fun x _ -> async {
            lock(monitor) (fun () -> 
                itemsReceived := (!itemsReceived |> Set.add x)
            )
        })

        let worktrackingQueue = new WorktrackingQueue<int,int,int>(groupingFunction, work, 100000, 50)

        let mutable ex =  null

        for item in [0..1] do
            worktrackingQueue.Add item |> Async.RunSynchronously

        worktrackingQueue.AsyncComplete () |> Async.RunSynchronously
        
        !itemsReceived |> should equal (Set.singleton 1)

    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Test speed of simple items`` () : unit =
        let groupingFunction i = Seq.singleton (i, fst i)

        let work = (fun _ _ -> Async.Sleep(10))
        let worktrackingQueue = new WorktrackingQueue<int,(int * int),(int * int)>(groupingFunction, work, 100000, 50)

        let items = 
            [0..10000] |> List.collect (fun group -> [0..10] |> List.map (fun item -> (group, item)))

        for item in items do
            worktrackingQueue.Add item |> Async.RunSynchronously

        worktrackingQueue.AsyncComplete () |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Items are received in order`` () : unit =
        let groupingFunction i = Seq.singleton (i, fst i)

        let monitor = new System.Object()
        let groupItems = ref Map.empty

        let error = ref "";
        let work group items = async {
            lock (monitor) (fun () -> 
                let groupItemsMap = !groupItems
                let mutable lastItem = -1
                if(groupItemsMap.ContainsKey group) then
                    lastItem <- groupItemsMap.[group]
                else
                    ()

                for (itemGroup,item) in items do
                   if(item <> (lastItem + 1)) then
                    error := (sprintf "Got event out of order %A %A" itemGroup item)
                   else 
                    ()
                   lastItem <- item

                groupItems := (groupItemsMap |> Map.add group lastItem)
            ) } 

        let worktrackingQueue = new WorktrackingQueue<int,(int * int),(int * int)>(groupingFunction, work, 100000, 50)

        let items = 
            [0..100] |> List.collect (fun group -> [0..10] |> List.map (fun item -> (group, item)))

        for item in items do
            worktrackingQueue.Add item |> Async.RunSynchronously

        worktrackingQueue.AsyncComplete () |> Async.RunSynchronously

        let errorResult = !error
        errorResult |> should equal ""
        
    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Completion function is called when item complete`` () : unit =
        let groupingFunction i = Seq.singleton (i, fst i)

        let tcs = new TaskCompletionSource<bool>()

        let completedItem = ref ("blank", "blank")
        let complete item = async {
            do! Async.Sleep(100)
            completedItem := item
            tcs.SetResult true
        }

        let worktrackingQueue = new WorktrackingQueue<string,(string * string),(string * string)>(groupingFunction, (fun _ _ -> Async.Sleep(100)), 100000,  10,  complete)
        worktrackingQueue.Add ("group", "item") |> Async.Start

        tcs.Task.Wait()

        !completedItem |> fst |> should equal "group"
        !completedItem |> snd |> should equal "item"

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Completion function is called immediately when an items results in 0 groups`` () : unit =
        let groupingFunction _ = Seq.empty

        let tcs = new TaskCompletionSource<bool>()

        let completedItem = ref ("blank", "blank")
        let complete item = async {
            do! Async.Sleep(100)
            completedItem := item
            tcs.SetResult true
        }

        let worktrackingQueue = new WorktrackingQueue<string,(string * string),(string * string)>(groupingFunction, (fun _ _ -> Async.Sleep(100)), 100000,  10,  complete)
        worktrackingQueue.Add ("group", "item") |> Async.Start

        tcs.Task.Wait()

        !completedItem |> fst |> should equal "group"
        !completedItem |> snd |> should equal "item"

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can run multiple items`` () : unit =
        let groupingFunction i = Seq.singleton (i, fst i)

        let completedItem = ref ("blank", "blank")
        let complete item = async {
            do! Async.Sleep(100)
            completedItem := item
        }

        let work (group:string) items = async {
                System.Console.WriteLine("Work item {0}", group)
            }

        let worktrackingQueue = new WorktrackingQueue<string,(string * string),(string * string)>(groupingFunction, work, 100000, 10, complete)
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

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given item split into 2 groups When complete Then Completion function is only called once`` () : unit =
        let groupingFunction i = [(i, 1); (i, 2)] |> Seq.ofList

        let completeCount = new CounterAgent()

        let complete item = async {
            // do! Async.Sleep(100)
            do! completeCount.Incriment 1
        }

        async {
            let worktrackingQueue = new WorktrackingQueue<int,(string * string),(string * string)>(groupingFunction, (fun _ _ -> Async.Sleep(100)), 100000, 10, complete)
            do! worktrackingQueue.Add ("group", "item")
            do! Async.Sleep 100
            do! worktrackingQueue.AsyncComplete()
            do! Async.Sleep 100
            let! count = completeCount.Get()
            count |> should equal 1
        } |> (fun f -> Async.RunSynchronously(f, 3000))

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given empty queue When complete Then returns immediately`` () : unit =
        let worktrackingQueue = new WorktrackingQueue<unit,string,string>( (fun i -> Seq.singleton (i, ())),(fun _ _ -> Async.Sleep(1)), 100000, 10,(fun _ -> Async.Sleep(1)))
        worktrackingQueue.AsyncComplete() |> Async.RunSynchronously

//    [<Property>]
//    let ``When Last Item From Last Group Complete Then Batch Complete``(items : List<(Guid * Set<int>)>) =
//        let state = WorktrackQueueState<int, obj>.Empty
//        let allAddedState = items |> List.fold (fun (s:WorktrackQueueState<int, obj>) (key, groups) -> s.Add(key, groups, async { return ()}) ) state
//        let replyChannel = new obj()
//        let (batchCreated, batchCreatedState) = allAddedState.CreateBatch(replyChannel)
//
//        let completeMessages =
//            items 
//            |> List.collect (fun (item, groups) -> groups |> Set.map (fun g -> (item, g)) |> Set.toList)
//
//        let applyComplete (_,_, queueState:WorktrackQueueState<int, obj>) (key, group) = 
//            queueState.ItemComplete(group, key)
//            
//        match (batchCreated, completeMessages) with
//        | (false, []) -> true
//        | (false, _) -> false
//        | (true,_) -> 
//            let startState = (None, List.empty, batchCreatedState)
//            let allCompleteState = 
//                completeMessages 
//                |> List.fold applyComplete startState
//
//            match allCompleteState with
//            | (_, [singleBatch], _) when singleBatch = replyChannel -> true
//            | _ -> false