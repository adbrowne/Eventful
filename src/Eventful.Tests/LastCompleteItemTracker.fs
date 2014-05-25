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

module LastCompleteItemTrackerTests = 
    [<Fact>]
    let ``Given empty When started Then lastComplete is None`` () : unit =  
        let tracker = LastCompleteItemTracker.Empty
        let result = tracker.Start 'a'
        result.LastComplete |> should be Null // none shows up as Null in FsUnit

    [<Fact>]
    let ``Given started 'a' When competed 'a' Then lastComplete is 'a'`` () : unit =  
        let tracker = LastCompleteItemTracker.Empty
        let result = ((tracker.Start 'a').Complete 'a')
        result.LastComplete |> should equal (Some 'a')

    [<Fact>]
    let ``Given [start 'a'; start 'b'; complete 'b'] When complete 'b' Then lastComplete is 'b'`` () : unit =  
        let result = 
            LastCompleteItemTracker.Empty
            |> LastCompleteItemTracker.start 'a'
            |> LastCompleteItemTracker.start 'b'
            |> LastCompleteItemTracker.complete 'b'
            |> LastCompleteItemTracker.complete 'a'

        result.LastComplete |> should equal (Some 'b')

    let rec insertAt list item index =
        match (index, list) with
        | (0, _) -> item::list
        | (_, x::xs) -> x::(insertAt xs item (index - 1))
        | (_, []) -> item::list

    let allCompleteOperationsGen = 
        let rec loop (acc : Item list) (value : int64) : Gen<Item list> = 
            match value with
            | 0L -> Gen.constant acc
            | length -> gen { 
                let startPosition = acc |> List.findIndex (fun x -> x = Start value)
                let! endPosition = Gen.choose(startPosition + 1, acc.Length)
                let finalAcc = insertAt acc (Item.Complete value) endPosition
                return! (loop finalAcc (value - 1L)) }

        Gen.sized (fun length -> loop ([1..length] |> List.map (fun x -> (int64 >> Start) x)) (int64 length))

    let allCompleteOperations = { new Arbitrary<Item list>() with 
        override x.Generator = allCompleteOperationsGen
        override x.Shrinker t = Seq.empty }

    let accumulator (acc : LastCompleteItemTracker<_>) op =
        match op with
        | Start value -> acc |> LastCompleteItemTracker.start value
        | Complete value -> acc |> LastCompleteItemTracker.complete value 

    [<Property>]
    let ``When all items complete last complete is equal to the maximum item`` () =
        forAll allCompleteOperations (fun (operations : Item list) -> 

            let result = operations |> List.fold accumulator LastCompleteItemTracker<_>.Empty

            if (operations.IsEmpty) then 
                result.LastComplete |> should be Null // none shows up as Null in FsUnit
            else
                let expected = 
                    operations
                    |> List.map (function 
                                | Start x -> x
                                | Complete x -> x)
                    |> List.max
                    |> int64
                    |> Some
                result.LastComplete |> should equal expected
        )


    [<Property>]
    let ``When last item is not complete then last complete is one less`` () =
        forAll allCompleteOperations (fun (allOperations : Item list) -> 
                if (allOperations.IsEmpty) then 
                    ()
                else
                    let operations = allOperations |> Seq.take (allOperations.Length - 1) |> Seq.toList
                    // last item must be a complete in a valid sequence
                    let (Complete missingValue) = allOperations |> Seq.last

                    let result = operations |> List.fold accumulator LastCompleteItemTracker<_>.Empty

                    if missingValue = 1L then
                        result.LastComplete |> should be Null // none shows up as Null in FsUnit
                    else
                        result.LastComplete |> should equal (Some (missingValue - 1L))
        )
