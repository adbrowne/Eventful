namespace Eventful

open System
open FSharpx.Collections

type internal GroupingBoundedQueueMessage<'TGroup, 'TItem, 'TResult when 'TGroup : comparison> = 
    | AsyncAdd of 'TGroup * 'TItem * AsyncReplyChannel<unit>
    | AsyncComplete of (('TGroup * List<'TItem> -> Async<'TResult>) * AsyncReplyChannel<'TResult>)
    | WorkComplete of 'TGroup

type GroupedItems<'TGroup, 'TItem when 'TGroup : comparison> = {
    Items: Map<'TGroup, List<'TItem>>
    ItemCount: int
}
with static member ContainsGroup (group: 'TGroup) (groupedItems:GroupedItems<'TGroup, 'TItem>) = 
        groupedItems.Items |> Map.containsKey group
     static member Add (group: 'TGroup) (item:'TItem) (groupedItems:GroupedItems<'TGroup, 'TItem>) =
        //Console.WriteLine("Adding Group: {0}", group)
        let newItems = 
            if(groupedItems.Items |> Map.containsKey group) then
                groupedItems.Items |> Map.add group (item :: groupedItems.Items.[group])
            else
                groupedItems.Items |> Map.add group (List.singleton item)
        //Console.WriteLine(sprintf "Added Group: %A" newItems)
        { Items = newItems; ItemCount = groupedItems.ItemCount + 1 }
     static member AddList (group: 'TGroup) (items:List<'TItem>) (groupedItems:GroupedItems<'TGroup, 'TItem>) =
        //Console.WriteLine("AddList Group: {0}", group)
        let newItems = 
            if(groupedItems.Items |> Map.containsKey group) then
                groupedItems.Items |> Map.add group (List.append items groupedItems.Items.[group])
            else
                groupedItems.Items |> Map.add group items
        { Items = newItems; ItemCount = groupedItems.ItemCount + items.Length }
     static member Remove (group: 'TGroup) (groupedItems:GroupedItems<'TGroup, 'TItem>) =
        if(groupedItems.Items |> Map.containsKey group) then
            //Console.WriteLine("Removed group {0}",group)
            let itemsInGroup = groupedItems.Items.[group].Length
            { 
                Items = groupedItems.Items |> Map.remove group
                ItemCount = groupedItems.ItemCount - itemsInGroup
            }
        else
            // Console.WriteLine("Group missing {0}",group)
            groupedItems
                
type RunningState<'TGroup, 'TItem when 'TGroup : comparison> = {
    AvailableWorkQueue : Queue<'TGroup>
    RunningGroups : Set<'TGroup>
    CurrentItems : GroupedItems<'TGroup, 'TItem>
    WaitingItems : GroupedItems<'TGroup, 'TItem>
}

type QueueMode =
    | Empty
    | NotFullWorkAvailable
    | NotFullNoWorkAvailable
    | FullWorkAvailable
    | FullNoWorkAvailable

type GroupingBoundedQueue<'TGroup, 'TItem, 'TResult when 'TGroup : comparison>(maxItems) =
    let empty = { AvailableWorkQueue = Queue.empty; RunningGroups = Set.empty; CurrentItems = { Items = Map.empty; ItemCount = 0}; WaitingItems = { Items = Map.empty; ItemCount = 0} }

    let getStateSummary runningState =
        let itemCount = runningState.CurrentItems.ItemCount + runningState.WaitingItems.ItemCount
        let workAvailable = not runningState.AvailableWorkQueue.IsEmpty
        match itemCount with
        | 0 -> Empty
        | count when count < maxItems -> 
            if(workAvailable) then
                NotFullWorkAvailable
            else
                NotFullNoWorkAvailable
        | _ ->
            if(workAvailable) then
                FullWorkAvailable
            else
                FullNoWorkAvailable

    let agentDef callback = Agent.Start(fun agent ->

        let rec emptyQueue(state) =
            agent.Scan(fun msg -> 
             match msg with
             | AsyncAdd(group, value, reply) -> Some(enqueue(group, value, reply, state))
             | _ -> None) 

         and notFullWorkAvailable(runningState) = async {
            let! msg = agent.Receive()
            match msg with 
            | AsyncAdd(group, value, reply) -> return! enqueue(group, value, reply, runningState)
            | AsyncComplete(work, reply) -> return! setWorking(work, reply, runningState)
            | WorkComplete(group) -> return! workComplete(group, runningState) }

         and notFullNoWorkAvailable(runningState) = 
            agent.Scan(fun msg -> 
             match msg with
             | AsyncAdd(group, value, reply) -> Some(enqueue(group, value, reply, runningState))
             | WorkComplete(group) -> Some(workComplete(group, runningState))
             | _ -> None)

        and fullWorkAvailable(runningState) = 
            agent.Scan(fun msg -> 
             match msg with
             | AsyncComplete(work, reply) -> Some(setWorking(work, reply, runningState))
             | WorkComplete(group) -> Some(workComplete(group, runningState))
             | _ -> None)

        and fullNoWorkAvailable(runningState) = 
            agent.Scan(fun msg -> 
             match msg with
             | WorkComplete(group) -> Some(workComplete(group, runningState))
             | _ -> None)

        and enqueue (group, value, reply, runningState) = async {
            reply.Reply()
            let newState = 
                if(runningState.RunningGroups |> Set.contains group) then
                    //Console.WriteLine("Group Added to Waiting Items {0}", group)
                    { runningState with WaitingItems = runningState.WaitingItems |> GroupedItems.Add group value }
                else
                    let newAvailableWorkQueue = 
                        if(runningState.CurrentItems |> GroupedItems.ContainsGroup group) then
                            runningState.AvailableWorkQueue
                        else
                            runningState.AvailableWorkQueue |> Queue.conj group
                    //Console.WriteLine("Group Added to Current Items {0}", group)
                    { runningState with 
                            AvailableWorkQueue = newAvailableWorkQueue
                            CurrentItems = runningState.CurrentItems |> GroupedItems.Add group value }
            return! chooseState(newState) }

        and workComplete (group, state) = async{
            // System.Console.WriteLine(sprintf "Work complete Group: %A" group)
            let completeCount = state.CurrentItems.Items.[group].Length
            let newState =  
                if(state.WaitingItems.Items |> Map.containsKey group) then
                    let waiting = state.WaitingItems.Items.[group]
                    { state with
                            RunningGroups = state.RunningGroups |> Set.remove group
                            CurrentItems = state.CurrentItems |> GroupedItems.Remove group |> GroupedItems.AddList group waiting
                            WaitingItems = state.WaitingItems |> GroupedItems.Remove group
                            AvailableWorkQueue = state.AvailableWorkQueue |> Queue.conj group
                    }
                else
                    { state with
                            RunningGroups = state.RunningGroups |> Set.remove group
                            CurrentItems = state.CurrentItems |> GroupedItems.Remove group
                    }
            callback completeCount
            return! chooseState(newState) }

        and setWorking (worker, reply, state : RunningState<'TGroup, 'TItem>) =
            let (group, newQueue) = state.AvailableWorkQueue |> Queue.uncons
            let items = state.CurrentItems.Items.[group]
            async {
                let! result = worker (group, items)
                reply.Reply result
                agent.Post <| WorkComplete group
            } |> Async.Start
            let newState = {
                state with AvailableWorkQueue = newQueue; RunningGroups = state.RunningGroups |> Set.add group
            }
            chooseState(newState)

        and chooseState(runningState) = 
            //Console.WriteLine(sprintf "Running State %A" runningState)
            let queueMode = getStateSummary runningState
            //Console.WriteLine(sprintf "Next State %A" queueMode)
            match queueMode with
            | Empty -> emptyQueue(empty)
            | NotFullWorkAvailable -> notFullWorkAvailable(runningState)
            | NotFullNoWorkAvailable -> notFullNoWorkAvailable(runningState)
            | FullWorkAvailable -> fullWorkAvailable(runningState)
            | FullNoWorkAvailable -> fullNoWorkAvailable(runningState)
           

        emptyQueue(empty)
    )

    let rec boundedWorkQueue = new BoundedWorkQueue<('TGroup * 'TItem)>(maxItems, postWorkToAgent)

    and agent = agentDef boundedWorkQueue.WorkComplete

    and postWorkToAgent (group : 'TGroup,item : 'TItem) = agent.PostAndAsyncReply((fun ch -> AsyncAdd(group, item, ch)))

    /// Asynchronously adds item to the queue. The operation ends when
    /// there is a place for the item. If the queue is full, the operation
    /// will block until some items are removed.
    member x.AsyncAdd(g: 'TGroup, v:'TItem, ?timeout) = 
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