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
| Start of int64
| Complete of int64

type State =
| Started
| CompletedButNotStarted
| StartedAndComplete

type StartCompleteTracker private (tracker : LastCompleteTracker<State,Item>) =
    static let mapping (msg : Item, state : State option) = 
        match (msg, state) with
        | (Start id, None) -> Some Started
        | (Start id, Some CompletedButNotStarted) -> None
        | (Start id, state) -> state // this is an error
        | (Complete id, None) -> Some CompletedButNotStarted
        | (Complete id, Some Started) -> None
        | (Complete id, state) -> state // this is an error

    static let empty = 
        new StartCompleteTracker(LastCompleteTracker.Empty mapping)

    static member Empty = empty

    member x.LastComplete = tracker.LastComplete

    member x.Process id operation = 
        let (isComplete, updated) = tracker.Process id operation
        new StartCompleteTracker(updated)

module LastCompleteTrackerTests = 
    [<Fact>]
    let ``Tracker starts at -1`` () : unit =
        StartCompleteTracker.Empty.LastComplete |> should equal -1L

    let rec insertAt list item index =
        match (index, list) with
        | (0, _) -> item::list
        | (_, x::xs) -> x::(insertAt xs item (index - 1))
        | (_, []) -> item::list
        
    let allCompleteOperationsGen = 
        let rec loop (acc : Item list) (length : int64) : Gen<Item list> = 
            match length with
            | 0L -> Gen.constant acc
            | length -> gen { 
                let! startPosition = Gen.choose(0, acc.Length)
                let value = length - 1L
                let acc' = insertAt acc (Start value) startPosition
                let! endPosition = Gen.choose(0, acc'.Length)
                let finalAcc = insertAt acc' (Complete value) endPosition
                return! (loop finalAcc value) }

        Gen.sized (fun length -> loop List.empty (int64 length))

    let allCompleteOperations = { new Arbitrary<Item list>() with 
        override x.Generator = allCompleteOperationsGen
        override x.Shrinker t = Seq.empty }

    let getId = function
    | Start id -> id
    | Complete id -> id

    [<Property>]
    let ``When all items complete last complete is equal to the maximum item`` () =
        forAll allCompleteOperations (fun (operations : Item list) -> 
            let maxItem = 
                if (operations.IsEmpty) then -1L
                else
                    operations
                    |> List.map (function 
                                | Start x -> x
                                | Complete x -> x)
                    |> List.max
                    |> int64

            let result = operations |> List.fold (fun (acc : StartCompleteTracker) op -> acc.Process (getId op) op) StartCompleteTracker.Empty

            result.LastComplete |> should equal maxItem
        )

    [<Property>]
    let ``When last item is not complete then last complete is one less`` () =
        forAll allCompleteOperations (fun (allOperations : Item list) -> 
            let (maxItem, operations) = 
                if (allOperations.IsEmpty) then 
                    (-1L, allOperations)
                else
                    let operations = allOperations |> Seq.take (allOperations.Length - 1) |> Seq.toList
                    let missingOperation = allOperations |> Seq.last
                    let missingOperationId = getId missingOperation
                    (missingOperationId - 1L, operations)

            let result = operations |> List.fold (fun (acc : StartCompleteTracker) op -> acc.Process (getId op) op) StartCompleteTracker.Empty

            result.LastComplete |> should equal maxItem
        )

    [<Fact>]
    let ``Timing for 1 million items`` () =
        let operations = 
            [0L..1000000L] |> List.collect (fun i -> [Start i; Complete i])

        let result = operations |> List.fold (fun (acc : StartCompleteTracker) op -> acc.Process (getId op) op) StartCompleteTracker.Empty

        result.LastComplete |> should equal 1000000L