namespace Eventful

open System
open FSharpx.Collections
open FSharpx

type SortedSet<'a> = System.Collections.Generic.SortedSet<'a>

[<CustomEquality; CustomComparison>]
type NotificationItem<'TItem when 'TItem : comparison> = {
    Item : 'TItem
    Unique : Guid // each item in the set must be unique
    Tag : string option
    Callback : Async<unit>
}
with
    static member Key p = 
        let { Item = key; Unique = unique } = p
        (key,unique)
    override x.Equals(y) = 
        equalsOn NotificationItem<'TItem>.Key x y
    override x.GetHashCode() = 
        hashOn NotificationItem<'TItem>.Key x
    interface System.IComparable with 
        member x.CompareTo y = compareOn NotificationItem<'TItem>.Key x y

type MutableLastCompleteTrackingState<'TItem when 'TItem : comparison> () =
    let started = new System.Collections.Generic.SortedSet<'TItem>()
    let completed = new System.Collections.Generic.SortedSet<'TItem>()
    let notifications = new System.Collections.Generic.SortedDictionary<'TItem, NotificationItem<'TItem> list>()

    let mutable nextToComplete = None
    let mutable currentLastComplete = None
    let mutable maxStarted = None
    let mutable incompleteCount = 0L

    // remove matching head sequences from xs and ys
    // returns sequences and highest matching value
    let removeMatchingHeads (xs : SortedSet<'TItem>) (ys : SortedSet<'TItem>) =
        let rec loop h = 
            match (xs |> Seq.tryHead, ys |> Seq.tryHead) with
            | Some x, Some y
                when x = y ->
                    xs.Remove(x) |> ignore
                    ys.Remove(y) |> ignore
                    loop (Some x)
            | _ -> h

        loop None

    member this.LastComplete = currentLastComplete

    member this.Start item =
        let shouldAdd = 
            match maxStarted with
            | None  -> true
            | Some i when item > i -> true
            | _ -> false
            
        if shouldAdd then
            let addedToStarted = started.Add(item) 
            incompleteCount <- incompleteCount + 1L
            
            match nextToComplete with
            | Some next when next > item ->
                nextToComplete <- Some item
            | None ->
                nextToComplete <- Some item
            | _ -> ()
            
            maxStarted <- Some item
            Some this
        else
            None

    member this.Complete item =
        if (started.Contains(item)) then
            let addedToComplete = completed.Add(item)

            incompleteCount <- incompleteCount - 1L

            if Some item = nextToComplete then
                let lastComplete' = removeMatchingHeads started completed

                currentLastComplete <-
                    match lastComplete', currentLastComplete with
                    | Some x, None -> Some x
                    | Some x, Some y when x > y -> Some x
                    | Some x, Some y -> currentLastComplete
                    | None, _ -> currentLastComplete

                nextToComplete <- (started |> Seq.tryHead)
            Some this
        else
            None

    member this.GetNotifications () = 
        let rec loop lastComplete callbacks = 
            match (notifications |> Seq.tryHead) with
            | Some kvp -> //({ Item = item; Callback = callback; Tag = tag; Unique = unique } as value) ->
                if(kvp.Key <= lastComplete) then
                    let newCallbacks = kvp.Value |> List.map (fun { Callback = callback } -> callback)
                    notifications.Remove kvp.Key |> ignore
                    loop lastComplete (newCallbacks@callbacks)
                else
                    callbacks
            | None -> 
                callbacks

        match currentLastComplete with
        | Some a -> (this, loop a List.empty)
        | None -> (this, List.empty)

    member this.AddNotification (item, tag, callback) =
        let uniqueId = Guid.NewGuid()
        let newValue = { Item = item; Unique = uniqueId; Tag = tag; Callback = callback}
        if(notifications.ContainsKey item) then
            let currentValues = notifications.Item item
            notifications.Remove item |> ignore
            notifications.Add (item, (newValue::currentValues))
        else
            notifications.Add (item, (List.singleton newValue))
            
        this.GetNotifications()

    static member Empty = new MutableLastCompleteTrackingState<'TItem>()

type LastCompleteItemMessage2<'TItem when 'TItem : comparison> = 
|    Start of ('TItem * AsyncReplyChannel<unit>) 
|    Complete of 'TItem
|    LastComplete of (AsyncReplyChannel<'TItem option>) 
|    Notify of ('TItem * string option * Async<unit>)

type LastCompleteItemAgent2<'TItem when 'TItem : comparison> (?name : string) = 
    let log = Common.Logging.LogManager.GetLogger(typeof<LastCompleteItemAgent2<_>>)

    let runCallbacks callbacks = async {
         for callback in callbacks do
            try
                do! callback
            with | e ->
                log.Error("Exception in notification callback", e) }

    let agent =
        let theAgent = Agent.Start(fun agent ->
            let rec loop (state : MutableLastCompleteTrackingState<'TItem>) = async {
                let! msg = agent.Receive()
                match msg with
                | Start (item, reply) ->
                    reply.Reply()

                    match state.Start item with
                    | Some state' ->
                        return! loop state'
                    | None ->
                        log.Error(sprintf "Item added out of order: %A" item)
                        return! loop state
                | Complete item ->
                    match state.Complete item with
                    | Some s ->
                        let (state', notificationCallbacks) = s.GetNotifications()

                        do! runCallbacks notificationCallbacks

                        return! loop state'
                    | None ->
                        log.Error(sprintf "Item completed before started: %A" item)
                        return! loop state
                | LastComplete reply ->
                    reply.Reply(state.LastComplete)
                    return! loop state
                | Notify (item, tag, callback) ->
                    let(state', notificationCallbacks) = state.AddNotification(item, tag, callback) 
                    do! runCallbacks notificationCallbacks

                    return! loop state'
            }

            loop MutableLastCompleteTrackingState<'TItem>.Empty
        )
        theAgent.Error.Add(fun exn -> 
            log.Error("Exception thrown by LastCompleteItemAgent2", exn))
        theAgent

    member x.LastComplete () : Async<'TItem option> =
        agent.PostAndAsyncReply((fun ch -> LastComplete(ch)))

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)

    member x.NotifyWhenComplete(item, tag : string option, callback :  Async<unit>) =
      agent.Post <| Notify (item, tag, callback)