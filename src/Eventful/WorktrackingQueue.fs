namespace Eventful

open System
open System.Collections.Generic

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * 'TItem
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorkqueueState<'TGroup, 'TItem when 'TGroup : comparison and 'TItem : comparison> = {
    Items: Map<'TItem, (Set<'TGroup> * Async<unit>)>
    Batches: (AsyncReplyChannel<unit> * Set<'TItem>) list
}

type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison and 'TItem : comparison>
    (
        maxGroups, 
        maxItems, 
        grouping : 'TItem -> Set<'TGroup>, 
        complete : 'TItem -> Async<unit>,
        workerCount,
        workAction : 'TGroup -> 'TItem seq -> Async<unit>
    ) =

    let queue = new GroupingBoundedQueue<'TGroup, 'TItem>(maxGroups, maxItems)

    let agent = Agent.Start(fun agent ->

        let rec loop(state : WorkqueueState<'TGroup, 'TItem>) =
            agent.Scan(fun msg -> 
                     match msg with
                     | Start (item, groups, complete, reply) -> 
                        let newItems = Map.add item (groups, complete) state.Items
                        Some(async {
                            for group in groups do
                                do! queue.AsyncAdd(group, item)
                            do reply.Reply()
                            return! loop ({ state with Items = newItems})
                        })
                     | Complete (group, item) -> 
                        let (groups, reply) = state.Items.[item]
                        let newGroups = groups |> Set.remove group
                        Some(async {
                                let! newState = async {
                                    if(newGroups.IsEmpty) then
                                        do! reply
                                        let itemSet = Set.singleton item
                                        let (emptyBatches, remainingBatches) = state.Batches |> List.partition (fun (_,items) -> items = itemSet)

                                        for (batchReply, _) in emptyBatches do
                                            batchReply.Reply()

                                        return { state with Items = state.Items |> Map.remove item; Batches = remainingBatches} 
                                    else
                                        return { state with Items = state.Items |> Map.add item (newGroups, reply) }
                                }
                                return! loop (newState)
                            }
                        )
                      | NotifyWhenAllComplete reply ->
                            let currentItems = state.Items |> Map.toList |> List.map fst |> Set.ofList
                            if(currentItems.IsEmpty) then
                                reply.Reply()
                                Some(loop(state))
                            else
                                Some(async {
                                        let newBatches = (reply, currentItems) :: state.Batches
                                        return! loop({state with Batches = newBatches })
                                     }
                                )
                )

        loop { Items = Map.empty; Batches = List.empty }
    )

    let workers = 
        for i in [1..workerCount] do
            async {
                while true do
                    let! (group, items) = queue.AsyncGet ()
                    do! workAction group items
                    for item in items do
                        agent.Post (Complete (group, item))
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