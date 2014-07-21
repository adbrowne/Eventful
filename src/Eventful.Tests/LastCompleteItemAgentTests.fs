namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
open FsCheck
open FsCheck.Prop
open FsCheck.Gen
open FsCheck.Xunit
open Eventful.Tests
open FSharpx
open FSharpx.Collections

type Item =
| Start of int64
| Complete of int64

module LastCompleteItemAgentTests = 
    let log = Common.Logging.LogManager.GetLogger(typeof<LastCompleteItemAgent<_>>)

    [<Fact>]
    let ``Can have two callbacks for the one item`` () : unit =  
        let lastCompleteTracker = new LastCompleteItemAgent<int64>(Guid.NewGuid().ToString())

        let monitor = new obj()

        let called = ref 0

        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        let callback = async {
            lock monitor (fun () -> 
                let result = System.Threading.Interlocked.Add(called, 1) 
                if(!called = 2) then tcs.SetResult(true)
                ()
            )
        }

        async {
            lastCompleteTracker.NotifyWhenComplete(1L, Some "My Thing", callback) 
            do! Async.Sleep(100)
            do! lastCompleteTracker.Start(1L)
            do! Async.Sleep(100)
            lastCompleteTracker.NotifyWhenComplete(1L, Some "My Thing2", callback) 
            do! Async.Sleep(100)
            lastCompleteTracker.Complete(1L)
            do! Async.Sleep(100)
            do! tcs.Task |> Async.AwaitTask |> Async.Ignore
            ()
        } |> (fun f -> Async.RunSynchronously(f, 1000))

        !called |> should equal 2

    let simulateTracking actions =
        let lastCompleteTracker = new LastCompleteItemAgent<int64>(Guid.NewGuid().ToString())

        let result = 
            async {
                for action in actions do
                    match action with
                    | Start i ->
                        do! lastCompleteTracker.Start(i)
                    | Complete i ->
                        lastCompleteTracker.Complete(i)

                return! lastCompleteTracker.LastComplete()
            } |> Async.RunSynchronously

        result
        
    let insertRandomly x xs = gen {
        let length = List.length xs

        let! position = Gen.choose(0, length - 1)

        let before = List.take position xs 
        let after = List.skip position xs

        return before @ [x] @ after
    }

    let validOperationsSequenceGen = 
        let rec loop (acc : Item list) (pendingItems : List<Item>) : Gen<Item list> = 
            match pendingItems with
            | [] -> gen {
                let! finalLength = Gen.choose(0, List.length acc)
                return! Gen.constant (acc |> List.rev |> Seq.take finalLength |> List.ofSeq) }
            | x::xs -> gen {
                let! pendingItems' = 
                    match x with
                    | Start i -> insertRandomly (Complete i) xs
                    | Complete i -> Gen.constant xs
                return! loop (x::acc) pendingItems'
            }

        Gen.sized (fun length -> loop List.empty (List.init length (fun i -> Start <| int64 i)))

    let allCompleteOperations = { new Arbitrary<Item list>() with 
        override x.Generator = validOperationsSequenceGen
        override x.Shrinker t = Seq.empty }

    let getId = function
    | Start id -> id
    | Complete id -> id

    [<Property>]
    let ``LastComplete is equal to the maximum complete pair for a valid sequence`` () =
        forAll allCompleteOperations (fun (operations : Item list) -> 
            let toValue = function 
                | Start x -> x
                | Complete x -> x

            let expectedLastComplete = 
                operations 
                |> List.filter (function | Start _ -> false | Complete _ -> true) // look at the complete items
                |> List.map toValue // get the value
                |> List.sort // sort as the order in which they are completed matters
                |> Seq.zip (Seq.initInfinite int64) // zip with the infinite perfect sequence
                |> Seq.takeWhile (fun (x, y) -> x = y) // grab the parts until the complete list has a gap
                // grab the last matching pair
                |> List.ofSeq 
                |> List.rev
                |> Seq.tryHead
                |> Option.map fst

            let result = simulateTracking operations

            match expectedLastComplete with
            | Some x ->
                result |> should equal expectedLastComplete
            | None ->
                result |> should be Null
        )

    let removeDuplicates xs =
        let acc (list, itemSet) item =
            if (itemSet |> Set.contains item) then
                (list, itemSet)
            else
                (item::list, itemSet |> Set.add item)
        xs |> List.fold acc (List.empty, Set.empty) |> fst |> List.rev

    [<Property>]
    let ``LastComplete ignores items started before their time or completed before started`` (operations : Item list) =
        let filterInvalidItems filtered item =
            match item with
            | Start i ->
                if filtered |> List.isEmpty then
                    [Start i]
                else
                    let maxStarted = filtered |> List.filter (function | Start _ -> true | _ -> false) |> List.max

                    if(maxStarted < item) then
                        Start i :: filtered
                    else
                        filtered
            | Complete i ->
                let existingOperationsSet = filtered |> Set.ofList
                if existingOperationsSet |> Set.contains (Start i) && existingOperationsSet |> Set.contains (Complete i) |> not then
                    Complete i :: filtered
                else
                    filtered

        let normalizedOperations = operations |> List.fold filterInvalidItems List.empty |> List.rev

        let result = simulateTracking operations
        let normalizedResult = simulateTracking normalizedOperations

        Assert.Equal(normalizedResult, result)