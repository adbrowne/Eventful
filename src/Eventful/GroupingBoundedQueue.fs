namespace Eventful

open System
open System.Collections.Generic

type Agent<'T> = MailboxProcessor<'T>

type internal GroupingBoundedQueueMessage<'TGroup, 'TItem> = 
    | AsyncAdd of 'TGroup * 'TItem * AsyncReplyChannel<unit> 
    | AsyncGet of AsyncReplyChannel<'TGroup * List<'TItem>>

type GroupingBoundedQueue<'TGroup, 'TItem when 'TGroup : comparison>(maxGroups, maxItems, workerCount : int, workAction : 'TGroup -> List<'TItem> -> Async<unit>) =

    [<VolatileField>]
    let mutable itemCount = 0
    let mutable groupCount = 0
    let items = new Dictionary<'TGroup, List<'TItem>>() 
    let groupQueue = new Queue<'TGroup>()

    let agent = Agent.Start(fun agent ->

        let rec emptyQueue() =
            agent.Scan(fun msg -> 
             match msg with
             | AsyncAdd(group, value, reply) -> Some(enqueueAndContinueWithReply(group, value, reply))
             | _ -> None) 

        and fullQueue() = 
            agent.Scan(fun msg ->
              match msg with 
              | AsyncGet(reply) -> Some(dequeueAndContinue(reply))
              | _ -> None )

        and runningQueue() = async {
            let! msg = agent.Receive()
            match msg with 
            | AsyncAdd(group, value, reply) -> return! enqueueAndContinueWithReply(group, value, reply)
            | AsyncGet(reply) -> return! dequeueAndContinue(reply) }

        and enqueueAndContinueWithReply (group, value, reply) = async {
            reply.Reply() 
            if(items.ContainsKey(group)) then
                items.[group].Add(value)
            else
                let list = new List<'TItem>()
                list.Add(value)
                items.Add(group, list)
                groupQueue.Enqueue group
            itemCount <- itemCount + 1
            return! chooseState() }

        and dequeueAndContinue (reply) = async {
            let group = groupQueue.Dequeue()
            let groupItems = items.[group]
            items.Remove(group) |> ignore
            reply.Reply(group, groupItems)
            itemCount <- itemCount - groupItems.Count
            return! chooseState() }

        and chooseState() = 
            if itemCount = 0 then emptyQueue()
            elif itemCount < maxItems && groupCount < maxGroups then runningQueue()
            else fullQueue()

        emptyQueue()
    )

    let workers = 
        for i in [1..workerCount] do
            async {
                while true do
                    let! (group, items) = agent.PostAndAsyncReply(AsyncGet)
                    do! workAction group items
            } |> Async.Start
        
    /// Asynchronously adds item to the queue. The operation ends when
    /// there is a place for the item. If the queue is full, the operation
    /// will block until some items are removed.
    member x.AsyncAdd(g: 'TGroup, v:'TItem, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> AsyncAdd(g, v, ch)), ?timeout=timeout)