namespace Eventful

open FSharpx.Collections
open System

type GroupAgentMessages<'TGroup, 'TItem> = 
| Enqueue of ('TItem * Async<unit> * AsyncReplyChannel<unit>)
| ConsumeBatch of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)
| BatchComplete

type WorkQueueMessage<'TGroup, 'TItem> = 
| QueueWork of Agent<GroupAgentMessages<'TGroup, 'TItem>>
| ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)

type ItemAgentMessages<'TGroup when 'TGroup : comparison> =
| Complete of 'TGroup
| NotifyWhenComplete of Async<unit>
| Shutdown

type CompletionAgentMessages<'T> =
| ItemStart of 'T
| ItemComplete of 'T
| AllComplete of AsyncReplyChannel<unit>

type MyQueue<'TGroup, 'TItem  when 'TGroup : comparison>() =

    let maxItems = 1000

    let log (value : string) =
        //Console.WriteLine value
        ()

    let workQueueAgent = Agent.Start(fun agent -> 
        let rec empty () = 
            agent.Scan(fun msg -> 
            match msg with
            | QueueWork agent -> Some(enqueue agent Queue.empty)
            | _ -> None) 
        and hasWork (queue : Queue<Agent<GroupAgentMessages<'TGroup, 'TItem>>>) = async {
            let! msg = agent.Receive()
            // Console.WriteLine(sprintf "MyQueue receiving %A" msg)

            match msg with
            | QueueWork agent -> return! enqueue agent queue
            | ConsumeWork(work, reply) -> 
                let (next, remaining) = queue |> Queue.uncons 
                next.Post (ConsumeBatch(work,reply))
                if(Queue.isEmpty remaining) then
                    return! empty ()
                else
                    return! hasWork remaining }
        and enqueue groupAgent queue = async {
            // Console.WriteLine(sprintf "Queueing %A" groupAgent)
            return! hasWork(queue |> Queue.conj groupAgent)
        }
            
        empty ()
    )

    let buildGroupAgent group (boundedCounter : BoundedWorkCounter) (workerQueue:Agent<WorkQueueMessage<'TGroup, 'TItem>>) = Agent.Start(fun agent -> 
        let rec loop running waiting = async {
            let! msg = agent.Receive()
            match msg with
            | Enqueue (item, complete, reply) -> return! enqueue((item, complete),running, waiting, reply)
            | ConsumeBatch(work, reply) -> return! consumeBatch(work, reply, running, waiting)
            | BatchComplete -> return! batchComplete(waiting) }
        and enqueue(item, running, waiting, reply) = async {
            if ((not running) && (Queue.isEmpty waiting)) then
               workerQueue.Post(QueueWork agent)
            else
                ()
            reply.Reply()
            return! (loop running (waiting |> Queue.conj item)) }
        and consumeBatch(work, reply, running, waiting) = async {
            async {
               let items = waiting |> Queue.toSeq |> List.ofSeq
               do! work(group, items |> Seq.map fst)
               reply.Reply()
               agent.Post BatchComplete
               for (_, complete) in items do
                do! complete
               boundedCounter.WorkComplete(items |> List.length)
            } |> Async.Start
            return! loop true Queue.empty }
        and batchComplete(waiting) = async {
            log <| sprintf "Batch complete. Waiting: %A" waiting
            if (waiting |> Queue.isEmpty) then
                return! loop false waiting
            else
                workerQueue.Post(QueueWork agent)
                return! loop false waiting }

        loop false Queue.empty
    )

    let itemAgent (groups:Set<'TGroup>) whenAllComplete = Agent.Start(fun agent ->
        let rec run (groups, toNotify) = async {
            let! msg = agent.Receive()
            match msg with
            | Complete group -> return! complete group groups toNotify 
            | NotifyWhenComplete notify -> return! run(groups, notify::toNotify)
            | Shutdown -> return () }
        and complete group groups toNotify = async {
            let newGroups = groups |> Set.remove group
            if (newGroups |> Set.isEmpty) then
                for notify in toNotify do
                    do! notify
                return! run(Set.empty, List.empty)
            else
                return! run(newGroups, toNotify) }
                    

        run (groups, [whenAllComplete])
    )

    let completionAgent = Agent.Start(fun agent -> 
        let rec run (items, itemsCompletedEarly, toNotify) = async { 
            let! msg = agent.Receive()
            match msg with
            | ItemStart (i) -> return! (itemStart i items itemsCompletedEarly toNotify)
            | ItemComplete i -> return! (itemComplete i items itemsCompletedEarly toNotify) 
            | AllComplete r -> return! (allComplete r items itemsCompletedEarly toNotify)}
        and itemStart item items itemsCompletedEarly toNotify = async {
            if itemsCompletedEarly |> Set.contains item then
                return! run(items, itemsCompletedEarly |> Set.remove item, toNotify)
            else
                return! run(items |> Set.add item, itemsCompletedEarly, toNotify) }
        and allComplete r items itemsCompletedEarly toNotify = async {
            if(items = Set.empty) then
                r.Reply()
                return! run(items, itemsCompletedEarly, toNotify)
            else 
                return! run(items, itemsCompletedEarly, (items, r) :: toNotify) }
        and itemComplete item items itemsCompletedEarly toNotify = async {
            let newItems = items |> Set.remove item
            let newItemsCompletedEarly =
                if(newItems = items) then
                    itemsCompletedEarly |> Set.add item
                else
                    itemsCompletedEarly

            let (doneCompletions, remainingCompletionSets) = 
                toNotify 
                |> List.map (fun (items, callback) -> (items |> Set.remove item, callback))
                |> List.partition (fun (items, callback) -> Set.isEmpty items)

            log <| sprintf "itemComplete: (%A, %A)" doneCompletions remainingCompletionSets

            for (_, callback) in doneCompletions do
                callback.Reply()
            return! run(newItems, newItemsCompletedEarly, remainingCompletionSets)
        }

        run (Set.empty, Set.empty, List.empty)
    )
        
    let rec dispatcherAgent = Agent.Start(fun agent -> 
        let ensureGroupAgent groupAgents group =
            if (groupAgents |> Map.containsKey group) then 
                groupAgents 
            else 
                groupAgents |> Map.add group (buildGroupAgent group boundedCounter workQueueAgent)

        let rec run(groupAgents, itemIndex) = async {
            let! (item, groups, reply : AsyncReplyChannel<unit>) = agent.Receive()
            let newGroupAgents = groups |> Set.fold ensureGroupAgent groupAgents
            let itemAgent = itemAgent groups (async { 
                log <| sprintf "item complete %A %d" item itemIndex 
                completionAgent.Post <| ItemComplete itemIndex
                })
            completionAgent.Post (ItemStart itemIndex)
            for group in groups do
                let groupAgent = newGroupAgents.[group]
                do! groupAgent.PostAndAsyncReply((fun ch -> Enqueue (item, async { itemAgent.Post (Complete group) } , ch)))

            reply.Reply()
            return! run(newGroupAgents, itemIndex + 1)
        }

        run (Map.empty, 0) )
    and boundedCounter = new BoundedWorkCounter(10000)

    member this.Add (item:'TItem, groups: Set<'TGroup>) =
        async {
            let groupCount = groups |> Set.count
            do! boundedCounter.Start groupCount
            // Console.WriteLine "About to add to dispatcher"
            do! dispatcherAgent.PostAndAsyncReply((fun ch -> (item, groups, ch)))
            // Console.WriteLine "Added to dispatcher"
        }

    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        async {
            // Console.WriteLine "About to add to consume"
            do! workQueueAgent.PostAndAsyncReply((fun ch -> ConsumeWork(work,ch)))
            // Console.WriteLine "Consumed"
        }

    member this.CurrentItemsComplete () = completionAgent.PostAndAsyncReply(fun ch -> AllComplete ch)