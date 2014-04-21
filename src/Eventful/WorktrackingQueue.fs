namespace Eventful

open System
open System.Collections.Generic

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        grouping : 'TItem -> Set<'TGroup>, 
        workAction : 'TGroup -> 'TItem seq -> Async<unit>,
        ?maxItems : int, 
        ?workerCount,
        ?complete : 'TItem -> Async<unit>
    ) =

    let _maxItems = maxItems |> getOrElse 1000
    let _workerCount = workerCount |> getOrElse 1
    let _complete = complete |> getOrElse (fun _ -> async { return () })

    let queue = new GroupingBoundedQueue<'TGroup, ('TItem*Guid), unit>(_maxItems)

    let agent = Agent.Start(fun agent ->

        let rec loop(state : WorktrackQueueState<'TGroup, AsyncReplyChannel<unit>>) = async {
         // Console.WriteLine(sprintf "State: %A" state)
         let! msg = agent.Receive()
         match msg with
         | Start (item, groups, complete, reply) -> 
            let itemKey = Guid.NewGuid()
            // Console.WriteLine(sprintf "Started %A" itemKey)
            return! async {
                for group in groups do
                    do! queue.AsyncAdd(group, (item,itemKey))
                reply.Reply()
                return! loop (state.Add(itemKey, groups, complete))
            }
         | Complete (group, itemKey) -> 
            // Console.WriteLine(sprintf "Completed %A" itemKey)
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
        loop WorktrackQueueState<_,_>.Empty
    )

    let doWork (group, items) = async {
         // Console.WriteLine(sprintf "Received %d items" (items |> List.length))
         do! workAction group (items |> List.map fst)
         for item in (items |> List.map snd) do
            // Console.WriteLine(sprintf "Posting completed %A" item)
            agent.Post (Complete (group, item))
    }

    let workers = 
        for i in [1.._workerCount] do
            async {
                while true do
                    do! queue.AsyncConsume doWork
            } |> Async.Start

    member this.Add (item:'TItem) =
        async {
            let groups = grouping item
            if groups |> Set.isEmpty then
                do! _complete item
            else
                do! agent.PostAndAsyncReply (fun ch -> Start (item, groups, _complete item, ch))
        }
    member this.AsyncComplete () =
        async {
            do! agent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete ch)
        }