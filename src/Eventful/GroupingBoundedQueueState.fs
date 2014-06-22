namespace Eventful

open System
open FSharpx.Collections

type GroupingBoundedQueueState<'TGroup, 'TItem when 'TGroup : comparison> private 
     (
        currentItems : Map<'TGroup, List<'TItem>>,
        waitingItems : Map<'TGroup, List<'TItem>>,
        runningGroups : Set<'TGroup>,
        availableWorkQueue : Queue<'TGroup>
     ) =
    static member Empty = new GroupingBoundedQueueState<'TGroup, 'TItem>(Map.empty, Map.empty, Set.empty, FSharpx.Collections.Queue.empty)

    member this.enqueue(group, item) =
        let appendToMap group itemToAppend map =
            let newGroupItems = 
                match map |> Map.tryFind group with
                | Some (items) -> item::items |> List.rev
                | None -> List.singleton item
            map |> Map.add group newGroupItems
            
        if(runningGroups |> Set.contains group) then
            let addedToWaitingItems = waitingItems |> appendToMap group item
            new GroupingBoundedQueueState<'TGroup, 'TItem>(currentItems, addedToWaitingItems, runningGroups, availableWorkQueue)
        else
            let newAvailableWorkQueue = 
                if(currentItems |> Map.containsKey group) then
                    availableWorkQueue
                else
                    availableWorkQueue |> Queue.conj group

            let nextCurrentItems = currentItems |> appendToMap group item
            new GroupingBoundedQueueState<'TGroup, 'TItem>(nextCurrentItems, waitingItems, runningGroups, newAvailableWorkQueue)
    member this.workComplete(group) = 
            let completeCount = 
                match currentItems |> Map.tryFind group with
                | Some items -> items |> List.length
                | None -> 0

            let nextState = 
                match waitingItems |> Map.tryFind group with
                | Some waiting -> 
                    let newCurrentItems = 
                        currentItems 
                        |> Map.remove group  // remove the items in the current group
                        |> Map.add group waiting // add the waiting items 
                    new GroupingBoundedQueueState<'TGroup, 'TItem>(newCurrentItems, waitingItems |> Map.remove group, runningGroups |> Set.remove group, availableWorkQueue |> Queue.conj group)
                |None -> 
                    new GroupingBoundedQueueState<'TGroup, 'TItem>(currentItems |> Map.remove group, waitingItems, runningGroups |> Set.remove group, availableWorkQueue)
            (completeCount, nextState)
    member this.dequeue() =
        let (group, nextAvailableWorkQueue) = availableWorkQueue |> Queue.uncons
        let items = currentItems.[group]
        let nextState = new GroupingBoundedQueueState<'TGroup, 'TItem>(currentItems, waitingItems, runningGroups |> Set.add group, nextAvailableWorkQueue)
        (group, items, nextState)
    member this.workAvailable = not availableWorkQueue.IsEmpty