namespace Eventful

type internal BoundedWorkQueueMessage<'T> = 
    | QueueWork of 'T * AsyncReplyChannel<unit> 
    | WorkComplete of int

type BoundedWorkQueue<'T>(maxItems, queue : 'T -> Async<unit>) =

    let agent = Agent.Start(fun agent ->

        let rec emptyQueue() =
            agent.Scan(fun msg -> 
             match msg with
             | QueueWork(item, reply) -> Some(enqueue(item, reply, 0))
             | _ -> None) 

         and running(state) = async {
            let! msg = agent.Receive()
            match msg with 
            | QueueWork(item, reply) -> return! enqueue(item, reply, state)
            | WorkComplete(count) -> return! workComplete(count, state) }

         and full(state) = 
            agent.Scan(fun msg -> 
            match msg with 
            | WorkComplete(count) -> Some(workComplete(count, state))
            | _ -> None )

        and enqueue (item, reply, state) = async {
            // System.Console.WriteLine(sprintf "Queueing item: %A state: %d" item state)
            do! queue item
            reply.Reply()
            return! chooseState(state + 1) }

        and workComplete (count, state) = 
            // System.Console.WriteLine(sprintf "Complete item: %d state: %d" count state)
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

    member x.QueueWork(item: 'T, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> QueueWork(item, ch)), ?timeout=timeout)

    member x.WorkComplete(count: int, ?timeout) = 
      agent.Post(WorkComplete(count))