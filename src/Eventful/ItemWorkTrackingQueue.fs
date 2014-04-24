namespace Eventful

open FSharpx.Collections
open System

type internal BoundedCounterMessage<'T> = 
    | Start of int * AsyncReplyChannel<unit> 
    | Complete of int

type BoundedCounter(maxItems) =

    let agent = Agent.Start(fun agent ->

        let rec emptyQueue() =
            agent.Scan(fun msg -> 
             match msg with
             | Start(count, reply) -> Some(enqueue(count, reply, 0))
             | _ -> None) 

         and running(state) = async {
            let! msg = agent.Receive()
            match msg with 
            | Start(count, reply) -> return! enqueue(count, reply, state)
            | Complete(count) -> return! workComplete(count, state) }

         and full(state) = 
            agent.Scan(fun msg -> 
            match msg with 
            | Complete(count) -> Some(workComplete(count, state))
            | _ -> None )

        and enqueue (count, reply, state) = async {
            reply.Reply()
            return! chooseState(state + count) }

        and workComplete (count, state) = 
            chooseState(state - count)

        and chooseState(state) = 
            if(state = 0) then
                emptyQueue()
            else if(state < maxItems) then
                running state
            else
                full state

        emptyQueue()
    )

    member x.Start(count, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (count, ch)), ?timeout=timeout)

    member x.WorkComplete(count: int, ?timeout) = 
      agent.Post(Complete(count))

type GroupAgentMessages<'TGroup, 'TItem> = 
| Enqueue of ('TItem * AsyncReplyChannel<unit>)
| ConsumeBatch of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)
| BatchComplete

type WorkQueueMessage<'TGroup, 'TItem> = 
| QueueWork of Agent<GroupAgentMessages<'TGroup, 'TItem>>
| ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)

type MyQueue<'TGroup, 'TItem  when 'TGroup : comparison>() =
    let maxItems = 1000

    let log (value : string) =
        Console.WriteLine value

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

    let buildGroupAgent group (boundedCounter : BoundedCounter) (workerQueue:Agent<WorkQueueMessage<'TGroup, 'TItem>>) = Agent.Start(fun agent -> 
        let rec loop running waiting = async {
            let! msg = agent.Receive()
            match msg with
            | Enqueue (item, reply) -> return! enqueue(item,running, waiting, reply)
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
               do! work(group, items)
               reply.Reply()
               agent.Post BatchComplete
               boundedCounter.WorkComplete(items |> List.length)
            } |> Async.Start
            return! loop true Queue.empty }
        and batchComplete(waiting) = async {
            // log <| sprintf "Batch complete. Waiting: %A" waiting
            if (waiting |> Queue.isEmpty) then
                return! loop false waiting
            else
                workerQueue.Post(QueueWork agent)
                return! loop false waiting }

        loop false Queue.empty
    )

    let rec dispatcherAgent = Agent.Start(fun agent -> 
        let ensureGroupAgent groupAgents group =
            if (groupAgents |> Map.containsKey group) then 
                groupAgents 
            else 
                groupAgents |> Map.add group (buildGroupAgent group boundedCounter workQueueAgent)

        let rec run(groupAgents) = async {
            let! (item, groups, reply : AsyncReplyChannel<unit>) = agent.Receive()
            let newGroupAgents = groups |> Set.fold ensureGroupAgent groupAgents
            for group in groups do
                let groupAgent = newGroupAgents.[group]
                do! groupAgent.PostAndAsyncReply((fun ch -> Enqueue (item, ch)))

            reply.Reply()
            return! run(newGroupAgents)
        }

        run (Map.empty) )
    and boundedCounter = new BoundedCounter(10000)

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