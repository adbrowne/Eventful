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

type Item =
| Start of int
| Complete of int

type State =
| Started
| CompletedButNotStarted
| StartedAndComplete

type LastCompleteTracker private (items : Map<int, State>) =
    static let empty =
        new LastCompleteTracker(Map.empty)

    static member Empty : LastCompleteTracker = empty

    member x.LastComplete = 
        let startedAndComplete = 
            items |> Map.filter (fun k v -> v = StartedAndComplete)
        if startedAndComplete |> Map.isEmpty then
            -1
        else
            (startedAndComplete |> Map.toSeq |> Seq.map fst |> Seq.max)

    member x.Process operation =
        match operation with
        | Start id -> 
            match items |> Map.tryFind id with
            | None -> new LastCompleteTracker(items |> Map.add id Started)
            | Some CompletedButNotStarted -> new LastCompleteTracker(items |> Map.add id StartedAndComplete)
            | Some _ -> x
        | Complete id -> 
            match items |> Map.tryFind id with
            | None -> new LastCompleteTracker(items |> Map.add id CompletedButNotStarted)
            | Some Started -> new LastCompleteTracker(items |> Map.add id StartedAndComplete)
            | Some _ -> x

module LastCompleteTrackerTests = 
    [<Fact>]
    let ``Tracker starts at -1`` () : unit =
        LastCompleteTracker.Empty.LastComplete |> should equal -1

    let rec insertAt list item index =
        match (index, list) with
        | (0, _) -> item::list
        | (_, x::xs) -> x::(insertAt xs item (index - 1))
        | (_, []) -> item::list
        
    let allCompleteOperationsGen = 
        let rec loop (acc : Item list) length : Gen<Item list> = 
            match length with
            | 0 -> Gen.constant acc
            | length -> gen { 
                let! startPosition = Gen.choose(0, acc.Length)
                let value = length - 1
                let acc' = insertAt acc (Start value) startPosition
                let! endPosition = Gen.choose(0, acc'.Length)
                let finalAcc = insertAt acc' (Complete value) endPosition
                return! (loop finalAcc value) }

        Gen.sized (fun length -> loop List.empty length)

    let allCompleteOperations = { new Arbitrary<Item list>() with 
        override x.Generator = allCompleteOperationsGen
        override x.Shrinker t = Seq.empty }

    [<Property>]
    let ``When all items complete last complete is equal to the maximum item`` () =
        forAll allCompleteOperations (fun (operations : Item list) -> 
            let maxItem = 
                if (operations.IsEmpty) then -1
                else
                    operations
                    |> List.map (function 
                                | Start x -> x
                                | Complete x -> x)
                    |> List.max
            let result = operations |> List.fold (fun (acc : LastCompleteTracker) op -> acc.Process op) LastCompleteTracker.Empty

            result.LastComplete |> should equal maxItem
        )