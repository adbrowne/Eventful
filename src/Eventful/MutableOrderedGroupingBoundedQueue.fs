namespace Eventful

open System

open FSharpx

type GroupEntry<'TItem> = {
    Items : List<Int64 * 'TItem>
    Processing : List<Int64 * 'TItem>
}
  
type MutableOrderedGroupingBoundedQueueMessages<'TGroup, 'TItem when 'TGroup : comparison> = 
  | AddItem of (seq<'TItem * 'TGroup> * Async<unit> option * AsyncReplyChannel<unit>)
  | ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<Async<unit>>)
  | GroupComplete of 'TGroup
  | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type internal MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem when 'TGroup : comparison> () =
    // normal .NET dictionary for performance
    // very mutable
    let groupItems = new System.Collections.Generic.SortedDictionary<'TGroup, GroupEntry<'TItem>>()

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
        groupItems.Remove group |> ignore
        groupItems.Add(group, value')
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

type MutableOrderedGroupingBoundedQueue<'TGroup, 'TItem when 'TGroup : comparison>(?maxItems, ?name : string) =
    let log = Common.Logging.LogManager.GetLogger(typeof<MutableOrderedGroupingBoundedQueue<_,_>>)

    let maxItems =
        match maxItems with
        | Some v -> v
        | None -> 10000
    
    let state = new MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem>()
    
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
            and enqueue (items, onComplete, reply) itemIndex = async {
                let indexedItems = Seq.zip items (Seq.initInfinite (fun x -> itemIndex + int64 x)) |> Seq.cache
                for ((item, group), index) in indexedItems do
                    state.AddItemToGroup (index, item) group
                    do! lastCompleteTracker.Start index
                reply.Reply()

                let nextIndex = 
                    if Seq.length indexedItems > 0 then
                        let lastIndex = indexedItems |> Seq.map snd |> Seq.last
                        match onComplete with
                        | Some a -> lastCompleteTracker.NotifyWhenComplete(lastIndex, None, a)
                        | None -> ()
                        lastIndex + 1L
                    else
                        itemIndex

                // there were not items keep moving
                if(nextIndex = itemIndex) then 
                    match onComplete with
                    | Some a -> do! a
                    | None -> ()

                return! (nextMessage nextIndex) }
            and groupComplete group itemIndex = async {
                state.GroupComplete group
                

                return! nextMessage itemIndex }
            and consume (workCallback, reply) itemIndex = async {

                let (nextKey, values) = state.ConsumeNext()
                let work =
                    async {
                        try
                            do! workCallback(nextKey,values.Items |> List.rev |> List.map snd) 
                        with | e ->
                            System.Console.WriteLine ("Error" + e.Message)
                        
                        for (i, _) in values.Items do
                            lastCompleteTracker.Complete i

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
            log.Error("Exception thrown by MutableOrderedGroupingBoundedQueueMessages", exn))
        theAgent
    
    let addTimer = Metrics.Metric.Timer("AddTimer", Metrics.Unit.None)
    member this.Add (input:'TInput, group: ('TInput -> (seq<'TItem * 'TGroup>)), ?onComplete : Async<unit>) =
        addTimer.Time(fun () ->
            async {
                let items = group input
                
                do! dispatcherAgent.PostAndAsyncReply(fun ch ->  AddItem (items, onComplete, ch))
                ()
            }
        )

    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        dispatcherAgent.PostAndAsyncReply(fun ch -> ConsumeWork(work, ch))

    member this.CurrentItemsComplete () = 
        dispatcherAgent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete(ch))