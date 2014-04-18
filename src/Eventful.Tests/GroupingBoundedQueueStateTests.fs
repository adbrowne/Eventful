namespace Eventful.Tests

open Eventful
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open System

module GroupingBoundedQueueStateTests = 
    [<Property>]
    let ``When all enqued and then each group dequed once we are complete`` (messages : List<string * int>) : bool =
        let emptyState = GroupingBoundedQueueState.Empty

        let enqueue (state:GroupingBoundedQueueState<string,int>) (group, value) =
            state.enqueue(group, value)
        let allEnqueued =
            messages |> List.fold enqueue emptyState

        let rec dequeueAll (state:GroupingBoundedQueueState<string,int>) sum = 
            match state.workAvailable with
            | false -> sum
            | true -> 
                let (group, items, nextState) = state.dequeue()
                dequeueAll nextState ((items |> List.sum) + sum)

        let sumFromQueue = dequeueAll allEnqueued 0

        let manualSum = messages |> List.map snd |> List.sum

        sumFromQueue = manualSum