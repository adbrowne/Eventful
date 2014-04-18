namespace Eventful

open System
open System.Collections.Generic

type WorktrackQueueState<'TGroup, 'TBatchToken when 'TGroup : comparison> private 
    (
        items : Map<Guid, (Set<'TGroup> * Async<unit>)>, 
        batches : ('TBatchToken * Set<Guid>) list
    ) = 
    static member Empty = new WorktrackQueueState<'TGroup, 'TBatchToken>(Map.empty, List.empty)
    member this.Add(key, groups, complete) = 
        if groups = Set.empty then
            this
        else 
            new WorktrackQueueState<'TGroup, 'TBatchToken>(
                items |> Map.add key (groups, complete), 
                batches
            )
    member this.ItemComplete(group, key) = 
        let (groups, reply) = items.[key]
        let remainingItemGroups = groups |> Set.remove group
        let groupComplete = remainingItemGroups.IsEmpty

        if(groupComplete) then
            let itemSet = Set.singleton key
            let (emptyBatches, remainingBatches) = batches |> List.partition (fun (_,items) -> items = itemSet)

            let nextRemainingBatches = remainingBatches |> List.map (fun (r,items) -> (r, (items |> Set.remove key)))

            let completedBatchReplies = emptyBatches |> List.map fst

            let nextQueueState = new WorktrackQueueState<_,_>(items |> Map.remove key, nextRemainingBatches)
            (Some reply, completedBatchReplies, nextQueueState)
        else
            let nextQueueState = new WorktrackQueueState<_,_>(items |> Map.add key (remainingItemGroups, reply), batches)
            (None, List.empty, nextQueueState)
    member this.CreateBatch(reply) = 
        let currentItems = items |> Map.toList |> List.map fst |> Set.ofList
        if(currentItems.IsEmpty) then
            (false, this)
        else
            let newBatches = (reply, currentItems) :: batches
            (true, new WorktrackQueueState<_,_>(items, newBatches))