namespace Eventful

type internal BoundedCounterMessage<'T> = 
    | Start of int * AsyncReplyChannel<unit> 
    | Complete of int

type BoundedWorkCounter(maxItems) =

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