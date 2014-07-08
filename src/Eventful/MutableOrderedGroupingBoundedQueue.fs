namespace Eventful

open System

type GroupEntry<'TItem> = {
    Items : List<Int64 * 'TItem>
}
  
type MutableOrderedGroupingBoundedQueueMessages<'TGroup, 'TItem when 'TGroup : comparison> = 
  | AddItem of (seq<'TItem * 'TGroup> * Async<unit> * AsyncReplyChannel<unit>)
  | ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)
  | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type MutableOrderedGroupingBoundedQueue<'TGroup, 'TItem when 'TGroup : comparison>(?maxItems) =
    let maxItems =
        match maxItems with
        | Some v -> v
        | None -> 10000
    
    // normal .NET dictionary for performance
    // very mutable
    let groupItems = new System.Collections.Generic.Dictionary<'TGroup, GroupEntry<'TItem>>()

    let lastCompleteTracker = new LastCompleteItemAgent2<int64>()

    let addItemToGroup item group =
        let (exists, value) = groupItems.TryGetValue(group)
        let value = 
            if exists then value
            else { Items = List.empty } 
        let value' = { value with Items = item::value.Items }
        groupItems.Remove group |> ignore
        groupItems.Add(group, value')
        ()

    let dispatcherAgent = Agent.Start(fun agent -> 
        let rec empty itemIndex = 
            agent.Scan(fun msg -> 
            match msg with
            | AddItem x -> Some (enqueue x itemIndex)
            | NotifyWhenAllComplete reply -> 
                lastCompleteTracker.NotifyWhenComplete(itemIndex, async { reply.Reply() } )
                Some(empty itemIndex)
            | _ -> None)
        and hasWork itemIndex =
            agent.Scan(fun msg ->
            match msg with
            | AddItem x -> Some <| enqueue x itemIndex
            | ConsumeWork x -> Some <| consume x itemIndex
            | NotifyWhenAllComplete reply ->
                lastCompleteTracker.NotifyWhenComplete(itemIndex, async { reply.Reply() } )
                Some(hasWork itemIndex))
        and enqueue (items, onComplete, reply) itemIndex = async {
            let indexedItems = Seq.zip items (Seq.initInfinite (fun x -> itemIndex + int64 x))
            for ((item, group), index) in indexedItems do
                addItemToGroup (index, item) group
                do! lastCompleteTracker.Start index
            reply.Reply()
            // todo watch out for no items
            let nextIndex = indexedItems |> Seq.map snd |> Seq.max
            return! hasWork (nextIndex) }
        and consume (workCallback, reply) itemIndex = async {
            let nextKey = groupItems.Keys |> Seq.head
            let values = groupItems.Item nextKey
            async {
                try
                    do! workCallback(nextKey,values.Items |> List.rev |> List.map snd) 
                with | e ->
                    System.Console.WriteLine ("Error" + e.Message)
                
                for (i, _) in values.Items do
                    lastCompleteTracker.Complete i

            } |> Async.StartAsTask |> ignore

            reply.Reply()
            groupItems.Remove(nextKey) |> ignore
            if(groupItems.Count = 0) then
                return! empty itemIndex
            else
                return! hasWork itemIndex
        }
        empty 0L )

    member this.Add (input:'TInput, group: ('TInput -> (seq<'TItem * 'TGroup>)), ?onComplete : Async<unit>) =
        async {
            let items = group input
            let onCompleteCallback = async {
                match onComplete with
                | Some callback -> return! callback
                | None -> return ()
            }
            do! dispatcherAgent.PostAndAsyncReply(fun ch ->  AddItem (items, onCompleteCallback, ch))
            ()
        }

    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        dispatcherAgent.PostAndAsyncReply(fun ch -> ConsumeWork(work, ch))

    member this.CurrentItemsComplete () = 
        dispatcherAgent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete(ch))