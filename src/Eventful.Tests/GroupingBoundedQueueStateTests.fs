namespace Eventful.Tests

open Eventful
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
open FsCheck
open FsCheck.Random
open FsCheck.Xunit
open System

type QueueAction =
| Enqueue of string * int
| Complete of string
| Dequeue

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


    [<Property(MaxTest = 100)>]
    let ``The next batch of a group should not be sent until the previous batch is complete`` (allActions : QueueAction list) : Property =
        let emptyState = GroupingBoundedQueueState.Empty

        let rec nextStep (actions : QueueAction list) (state:GroupingBoundedQueueState<string, int>) (working : Set<string>) = 
            match (actions) with
            | [] -> true |@ "complete"
            | (Enqueue (group, item))::xs -> 
                let nextState = state.enqueue(group,item)
                nextStep xs nextState working
            | Dequeue::xs -> 
                if(state.workAvailable) then 
                    let (group, items, nextState) = state.dequeue()
                    if working |> Set.contains group then
                        false |@ sprintf "group dequeued while being worked on %A" group
                    else
                        nextStep xs nextState (Set.add group working)
                else 
                    nextStep xs state working
            | Complete(group)::xs -> 
                if (working |> Set.contains group) then
                    let (completeCount, nextState) = state.workComplete(group)
                    let nowWorking = Set.remove group working
                    nextStep xs nextState nowWorking
                else
                    nextStep xs state working

        nextStep allActions GroupingBoundedQueueState.Empty Set.empty