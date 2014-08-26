namespace Eventful.Tests

open Eventful
open Eventful.Testing.TestHelpers
open Xunit
open FsUnit.Xunit
open FsCheck.Xunit
open FsCheck
open FsCheck.Prop

module OperationSetCompleteTests = 

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``When single item is completed then callback is run`` () : unit =
        let isDone = ref false
        let operationTracker = new OperationSetComplete<int>(Set.singleton 1, async { isDone := true })

        operationTracker.Complete(1)

        (fun () -> !isDone) |> assertTrueWithin 200  "Should be complete" 

    [<Property>]
    [<Trait("category", "unit")>]
    let ``When set is all complete then callback is run`` (input:Set<int>) : unit =
        let isDone = ref false
        let operationTracker = new OperationSetComplete<int>(input, async { isDone := true })

        for i in input do
            operationTracker.Complete(i)

        (fun () -> !isDone) |> assertTrueWithin 200  "Should be complete" 

    [<Property>]
    [<Trait("category", "unit")>]
    let ``When set not complete then callback is NOT run`` (input:Set<int>) =
        let isDone = ref false

        let setIsNotEmpty = input |> Set.isEmpty |> not

        setIsNotEmpty ==> (fun () -> 
            let operationTracker = new OperationSetComplete<int>(input, async { isDone := true })

            let toRemove = input |> Set.toSeq |> Seq.head
            let inputMissingOne = input |> Set.remove toRemove
            for i in inputMissingOne do
                operationTracker.Complete(i)

            System.Threading.Thread.Sleep(10)

            not !isDone)