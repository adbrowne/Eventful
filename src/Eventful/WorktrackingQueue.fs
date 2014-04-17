namespace Eventful

open System
open System.Collections.Generic

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorktrackQueueState<'TGroup when 'TGroup : comparison> private 
    (
        items : Map<Guid, (Set<'TGroup> * Async<unit>)>, 
        batches : (AsyncReplyChannel<unit> * Set<Guid>) list
    ) = 
    static member Empty = new WorktrackQueueState<'TGroup>(Map.empty, List.empty)
    member this.Add(key, groups, complete) = 
        new WorktrackQueueState<'TGroup>(
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

            let completedBatchReplies = emptyBatches |> List.map fst

            let nextQueueState = new WorktrackQueueState<_>(items |> Map.remove key, remainingBatches)
            (Some reply, completedBatchReplies, nextQueueState)
        else
            let nextQueueState = new WorktrackQueueState<_>(items |> Map.add key (remainingItemGroups, reply), batches)
            (None, List.empty, nextQueueState)
    member this.CreateBatch(reply) = 
        let currentItems = items |> Map.toList |> List.map fst |> Set.ofList
        if(currentItems.IsEmpty) then
            (false, this)
        else
            let newBatches = (reply, currentItems) :: batches
            (true, new WorktrackQueueState<_>(items, newBatches))


type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        maxItems, 
        grouping : 'TItem -> Set<'TGroup>, 
        complete : 'TItem -> Async<unit>,
        workerCount,
        workAction : 'TGroup -> 'TItem seq -> Async<unit>
    ) =

    let queue = new GroupingBoundedQueue<'TGroup, ('TItem*Guid), unit>(maxItems)

    let agent = Agent.Start(fun agent ->

        let rec loop(state : WorktrackQueueState<'TGroup>) = async {
         let! msg = agent.Receive()
         match msg with
         | Start (item, groups, complete, reply) -> 
            let itemKey = Guid.NewGuid()
            return! async {
                for group in groups do
                    do! queue.AsyncAdd(group, (item,itemKey))
                reply.Reply()
                return! loop (state.Add(itemKey, groups, complete))
            }
         | Complete (group, itemKey) -> 
            let (completeCallback, completedBatchReplies, nextState) = state.ItemComplete(group, itemKey)
            for reply in completedBatchReplies do
                reply.Reply()

            match completeCallback with
            | Some reply -> do! reply
            | None -> ()

            return! loop(nextState)
         | NotifyWhenAllComplete reply ->
            let (batchCreated, nextState) = state.CreateBatch(reply)
            if(batchCreated) then
                return! loop(nextState)
            else 
                reply.Reply()
                return! loop(nextState)
        }
        loop WorktrackQueueState<_>.Empty
    )

    let workers = 
        for i in [1..workerCount] do
            async {
                while true do
                    do! queue.AsyncConsume (fun (group, items) -> async {
                                                                             do! workAction group (items |> List.map fst)
                                                                             for item in (items |> List.map snd) do
                                                                                agent.Post (Complete (group, item))
                                                                        })
            } |> Async.Start

    member this.Add (item:'TItem) =
        async {
            let groups = grouping item
            do! agent.PostAndAsyncReply (fun ch -> Start (item, groups, complete item, ch))
        }
    member this.AsyncComplete () =
        async {
            do! agent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete ch)
        }