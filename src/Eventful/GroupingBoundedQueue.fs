namespace Eventful

open System
open FSharpx.Collections

type internal GroupingBoundedQueueMessage<'TGroup, 'TItem, 'TResult when 'TGroup : comparison> = 
    | AsyncAdd of 'TGroup * 'TItem * AsyncReplyChannel<unit>
    | AsyncComplete of (('TGroup * List<'TItem> -> Async<'TResult>) * AsyncReplyChannel<'TResult>)
    | WorkComplete of 'TGroup

type GroupingBoundedQueue<'TGroup, 'TItem, 'TResult when 'TGroup : comparison>(maxItems) =

    let agentDef callback = Agent.Start(fun agent ->

        let rec workAvailable(state:GroupingBoundedQueueState<'TGroup,'TItem>) = async {
            let! msg = agent.Receive()
            match msg with 
            | AsyncAdd(group, value, reply) -> return! enqueue(group, value, reply, state)
            | AsyncComplete(work, reply) -> return! setWorking(work, reply, state)
            | WorkComplete(group) -> return! workComplete(group, state) 
            }
        and noWorkAvailable(state) = 
            agent.Scan(fun msg -> 
             match msg with
             | AsyncAdd(group, value, reply) ->  Some(enqueue(group, value, reply, state))
             | WorkComplete(group) -> Some(workComplete(group, state))
             | _ -> None)

        and enqueue ((group:'TGroup), (value:'TItem), reply, state) = async {
            reply.Reply()
            let nextState = state.enqueue(group, value)
            return! chooseState(nextState) }

        and workComplete ((group:'TGroup), state) = async{
            let (completeCount, nextState) = state.workComplete(group)
            callback completeCount
            return! chooseState(nextState) }

         and setWorking (worker, reply, state) =
            let (group, items, nextState) = state.dequeue()
            async {
                let! result = worker (group, items)
                reply.Reply result
                agent.Post <| WorkComplete group
            } |> Async.Start
            chooseState(nextState)

        and chooseState(state) = 
            if(state.workAvailable) then
                workAvailable(state)
            else
                noWorkAvailable(state)

        noWorkAvailable(GroupingBoundedQueueState<'TGroup, 'TItem>.Empty)
    )

    let rec boundedWorkQueue = new BoundedWorkQueue<'TGroup * 'TItem>(maxItems, postWorkToAgent)

    and agent = agentDef boundedWorkQueue.WorkComplete

    and postWorkToAgent (group,item) = agent.PostAndAsyncReply((fun ch -> AsyncAdd(group, item, ch)))

    /// Asynchronously adds item to the queue. The operation ends when
    /// there is a place for the item. If the queue is full, the operation
    /// will block until some items are removed.
    member x.AsyncAdd(g : 'TGroup, v : 'TItem, ?timeout) = 
        boundedWorkQueue.QueueWork ((g,v), ?timeout=timeout)

    /// Asynchronously gets item from the queue. If there are no items
    /// in the queue, the operation will block until items are added.
    // member x.AsyncGet(?timeout) = 
      // agent.PostAndAsyncReply(AsyncGet, ?timeout=timeout)
      //async {
      //  return 
      //}

    /// Asynchronously gets item from the queue. If there are no items
    /// in the queue, the operation will block until items are added.
    member x.AsyncConsume(worker : ('TGroup * list<'TItem>) -> Async<'TResult>, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> AsyncComplete(worker, ch)), ?timeout=timeout)