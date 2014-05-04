namespace Eventful

type internal BoundedCounterMessage<'T> = 
    | Start of AsyncReplyChannel<int64> 
    | GetCurrentIndex of AsyncReplyChannel<int64> 
    | Complete

type BoundedWorkCounter(maxItems) =

    let agent = Agent.Start(fun agent ->

        let rec emptyQueue lastIndex =
            agent.Scan(fun msg -> 
             match msg with
             | Start(reply) -> Some(enqueue(reply, (lastIndex,0)))
             | GetCurrentIndex reply -> 
                reply.Reply(lastIndex)
                Some(emptyQueue lastIndex) 
             | _ -> None) 

         and running (lastIndex, counter) = async {
            let! msg = agent.Receive()
            match msg with 
            | Start(reply) -> return! enqueue(reply, (lastIndex, counter))
            | GetCurrentIndex reply -> 
                reply.Reply lastIndex 
                return! running (lastIndex, counter)
            | Complete -> return! workComplete (lastIndex, counter) }

         and full (lastIndex, counter) = 
            agent.Scan(fun msg -> 
            match msg with 
            | Complete -> Some (workComplete (lastIndex, counter))
            | GetCurrentIndex reply -> 
                reply.Reply(lastIndex)
                Some(emptyQueue lastIndex) 
            | _ -> None )

        and enqueue (reply, (lastIndex, running)) = async {
            let nextIndex = lastIndex + 1L
            reply.Reply(nextIndex)
            return! chooseState (nextIndex, running + 1) }

        and workComplete (lastIndex : int64, counter : int) = 
            chooseState(lastIndex, counter - 1)

        and chooseState (lastIndex, counter) = 
            if(counter = 0) then
                emptyQueue lastIndex
            else if(counter < maxItems) then
                running (lastIndex, counter)
            else
                full (lastIndex, counter)

        emptyQueue -1L
    )

    member x.Start(count, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (ch)), ?timeout=timeout)

    member x.GetCurrentIndex(?timeout) = 
      agent.PostAndAsyncReply((fun ch -> GetCurrentIndex (ch)), ?timeout=timeout)

    member x.WorkComplete(?timeout) = 
      agent.Post(Complete)