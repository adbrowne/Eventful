namespace Eventful

open System

open FSharpx

type GroupEntryItem<'TItem> = Int64 * 'TItem * Option<OperationSetComplete<Int64>>
type GroupEntry<'TItem> = {
    Items : List<GroupEntryItem<'TItem>>
    Processing : List<GroupEntryItem<'TItem>>
}
  
type MutableOrderedGroupingBoundedQueueMessages<'TGroup, 'TItem> = 
  | AddItem of ((unit -> (seq<'TItem * 'TGroup>)) * Async<unit> option)
  | ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<Async<unit>>)
  | GroupComplete of 'TGroup
  | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type internal MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem> 
    (
        ?groupComparer : System.Collections.Generic.IComparer<'TGroup>
    ) =
    // normal .NET dictionary for performance
    // very mutable
    let groupItems = 
        match groupComparer with
        | Some c -> new System.Collections.Generic.SortedDictionary<'TGroup, GroupEntry<'TItem>>(c)
        | None -> new System.Collections.Generic.SortedDictionary<'TGroup, GroupEntry<'TItem>>()

    let workQueue = new System.Collections.Generic.Queue<'TGroup>()

    let mutable itemIndex = 0L
    let mutable lastIndex = -1L

    member x.GetGroupItemsCount () =
        let g = groupItems
        g.Count

    member x.GetWorkQueueCount () =
        workQueue.Count

    member x.AddItemToGroup item group =
        let (exists, value) = groupItems.TryGetValue(group)
        let value = 
            if exists then
                value
            else 
                workQueue.Enqueue group
                { Items = List.empty; Processing = List.empty } 
        let value' = { value with Items = item::value.Items }
        groupItems.[group] <- value'
        ()

    member x.LastIndex () = lastIndex

    member x.GroupComplete group =
        let values = groupItems.Item group
        if (not (values.Items |> List.isEmpty)) then
            workQueue.Enqueue(group)
        else
            groupItems.Remove group |> ignore

    member x.ConsumeNext () =
        let nextKey = workQueue.Dequeue()
        let values = groupItems.Item nextKey
        let newValues = { values with Items = List.empty; Processing = values.Items }
        groupItems.Remove(nextKey) |> ignore
        groupItems.Add(nextKey, newValues)
        (nextKey, values)

type MutableOrderedGroupingBoundedQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        ?maxItems, 
        ?name : string,
        ?groupComparer : System.Collections.Generic.IComparer<'TGroup>
    ) =
    let log = createLogger <| sprintf "MutableOrderedGroupingBoundedQueue<%s,%s>" typeof<'TGroup>.Name typeof<'TItem>.Name

    let maxItems =
        match maxItems with
        | Some v -> v
        | None -> 10000
    
    let state = 
        match groupComparer with
        | Some c -> new MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem>(c)
        | None -> new MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem>()
    
    let activeGroupsGauge = Metrics.Metric.Gauge(sprintf "Active Groups %A" name, state.GetGroupItemsCount >> float, Metrics.Unit.Items)
    let workQueueGauge = Metrics.Metric.Gauge(sprintf "Work Queue Size %A" name, state.GetWorkQueueCount >> float, Metrics.Unit.Items)

    let lastCompleteTracker = 
        match name with
        | Some name -> new LastCompleteItemAgent<int64>(name)
        | None -> new LastCompleteItemAgent<int64>() 

    let workerCallbackName = sprintf "Worker callback %A" name

    let dispatcherAgent = 
        let theAgent = Agent.Start(fun agent -> 
            let rec empty itemIndex = 
                agent.Scan(fun msg -> 
                match msg with
                | AddItem x -> Some (enqueue x itemIndex)
                | NotifyWhenAllComplete reply -> 
                    if(itemIndex = 0L) then
                        reply.Reply()
                    else 
                        lastCompleteTracker.NotifyWhenComplete(state.LastIndex(), Some "NotifyWhenComplete empty", async { reply.Reply() } )
                    Some(empty itemIndex)
                | GroupComplete group -> Some(groupComplete group itemIndex)
                | _ -> None)
            and hasWork itemIndex =
                agent.Scan(fun msg ->
                match msg with
                | AddItem x -> Some <| enqueue x itemIndex
                | ConsumeWork x -> Some <| consume x itemIndex
                | GroupComplete group -> Some(groupComplete group itemIndex)
                | NotifyWhenAllComplete reply ->
                    lastCompleteTracker.NotifyWhenComplete(itemIndex - 1L, Some "NotifyWhenComplete hasWork", async { reply.Reply() } )
                    Some(hasWork itemIndex))
            and full itemIndex = 
                agent.Scan(fun msg -> 
                match msg with
                | ConsumeWork x -> Some <| consume x itemIndex
                | AddItem x -> None
                | NotifyWhenAllComplete reply -> 
                    if(itemIndex = 0L) then
                        reply.Reply()
                    else 
                        lastCompleteTracker.NotifyWhenComplete(itemIndex - 1L, Some "NotifyWhenComplete empty", async { reply.Reply() } )
                    Some(empty itemIndex)
                | GroupComplete group -> Some(groupComplete group itemIndex))
            and enqueue (itemsF :(unit -> (seq<'TItem * 'TGroup>)), onComplete) itemIndex = async {
                
                try
                    let items = itemsF ()
                    let indexedItems = Seq.zip items (Seq.initInfinite (fun x -> itemIndex + int64 x)) |> Seq.cache
                    let indexSet = indexedItems |> Seq.map snd |> Set.ofSeq

                    let nextIndex = itemIndex + (indexSet |> Set.count |> int64)

                    let setCompleteTracker = 
                        match onComplete with
                        | Some a ->
                            Some (new OperationSetComplete<int64>(indexSet, a))
                        | None -> None

                    for ((item, group), index) in indexedItems do
                        state.AddItemToGroup (index, item, setCompleteTracker) group
                        do! lastCompleteTracker.Start index

                    return! (nextMessage nextIndex) 
                with | e -> 
                    log.ErrorWithException <| lazy("Exception thrown enqueueing item", e)
                    return! nextMessage itemIndex
                }
            and groupComplete group itemIndex = async {
                state.GroupComplete group
                return! nextMessage itemIndex }
            and consume (workCallback, reply) itemIndex = async {
                let (nextKey, values) = state.ConsumeNext()
                let work =
                    async {
                        try
                            let items =
                                values.Items 
                                // items are added to the front as they
                                // come in reverse them here so they match the order 
                                // of arrival
                                |> List.rev 
                                |> List.map (fun (_,a,_) -> a)
                            do! workCallback(nextKey, items) 
                        with | e ->
                            System.Console.WriteLine ("Error" + e.Message)
                        
                        for (i, _, groupCompleteTracker) in values.Items do
                            lastCompleteTracker.Complete i
                            match groupCompleteTracker with
                            | Some t -> t.Complete i
                            | None -> ()

                        agent.Post <| GroupComplete nextKey
                    }

                reply.Reply work

                return! nextMessage itemIndex }
            and nextMessage itemIndex = async {
                let currentQueueSize = state.GetWorkQueueCount()
                if(currentQueueSize = 0) then
                    return! empty itemIndex
                elif currentQueueSize >= maxItems then
                    return! full itemIndex   
                else
                    return! hasWork itemIndex
            }
            empty 0L )
        theAgent.Error.Add(fun exn -> 
            log.ErrorWithException <| lazy("Exception thrown by MutableOrderedGroupingBoundedQueueMessages", exn))
        theAgent

    let queueFullEvent = new Event<_>()    

    /// fired each time a full queue is detected
    [<CLIEvent>]
    member this.QueueFullEvent = queueFullEvent.Publish

    member this.Add (input:'TInput, group: ('TInput -> (seq<'TItem * 'TGroup>)), ?onComplete : Async<unit>) =
        async {
            while(state.GetWorkQueueCount() + dispatcherAgent.CurrentQueueLength > maxItems) do
                queueFullEvent.Trigger()
                do! Async.Sleep(10)
            dispatcherAgent.Post <| AddItem ((fun () -> group input), onComplete) }

    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        dispatcherAgent.PostAndAsyncReply(fun ch -> ConsumeWork(work, ch))

    member this.CurrentItemsComplete () = 
        dispatcherAgent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete(ch))