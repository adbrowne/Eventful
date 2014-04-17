namespace Eventful

open System
open System.Collections.Generic

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorkqueueState<'TGroup, 'TItemKey when 'TGroup : comparison and 'TItemKey : comparison> = {
    Items: Map<'TItemKey, (Set<'TGroup> * Async<unit>)>
    Batches: (AsyncReplyChannel<unit> * Set<'TItemKey>) list
}

type WorktrackQueueState<'TGroup, 'TItemKey when 'TGroup : comparison and 'TItemKey : comparison> () = 
    let items : Map<'TItemKey, (Set<'TGroup> * Async<unit>)> = Map.empty
    let batches : (AsyncReplyChannel<unit> * Set<'TItemKey>) list = List.empty

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

        let rec loop(state : WorkqueueState<'TGroup, Guid>) = async {
         let! msg = agent.Receive()
         match msg with
         | Start (item, groups, complete, reply) -> 
            let itemKey = Guid.NewGuid()
            let newItems = Map.add itemKey (groups, complete) state.Items
            return! async {
                for group in groups do
                    do! queue.AsyncAdd(group, (item,itemKey))
                reply.Reply()
                return! loop ({ state with Items = newItems})
            }
         | Complete (group, itemKey) -> 
            let (groups, reply) = state.Items.[itemKey]
            let newGroups = groups |> Set.remove group
            let! newState = async {
                if(newGroups.IsEmpty) then
                    do! reply
                    let itemSet = Set.singleton itemKey
                    let (emptyBatches, remainingBatches) = state.Batches |> List.partition (fun (_,items) -> items = itemSet)

                    for (batchReply, _) in emptyBatches do
                        batchReply.Reply()

                    return { state with Items = state.Items |> Map.remove itemKey; Batches = remainingBatches} 
                else
                    return { state with Items = state.Items |> Map.add itemKey (newGroups, reply) }
            }
            return! loop (newState)
          | NotifyWhenAllComplete reply ->
                let currentItems = state.Items |> Map.toList |> List.map fst |> Set.ofList
                if(currentItems.IsEmpty) then
                    reply.Reply()
                    return! loop(state)
                else
                    let newBatches = (reply, currentItems) :: state.Batches
                    return! loop({state with Batches = newBatches })
        }
        loop { Items = Map.empty; Batches = List.empty }
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