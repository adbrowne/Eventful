﻿namespace Eventful

open System
open FSharpx.Collections
open FSharpx

type SortedSet<'a> = System.Collections.Generic.SortedSet<'a>

type NotificationItem<'TItem> = {
    Item : 'TItem
    Tag : string option
    Callback : Async<unit>
}

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
            match (xs |> tryHead, ys |> tryHead) with
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

                nextToComplete <- (started |> tryHead)
            Some this
        else
            None

    member this.GetNotifications () = 
        let rec loop lastComplete callbacks = 
            match (notifications |> tryHead) with
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
        let newValue = { Item = item; Tag = tag; Callback = callback}
        if(notifications.ContainsKey item) then
            let currentValues = notifications.Item item
            notifications.Remove item |> ignore
            notifications.Add (item, (newValue::currentValues))
        else
            notifications.Add (item, (List.singleton newValue))
            
        this.GetNotifications()

    static member Empty = new MutableLastCompleteTrackingState<'TItem>()

type LastCompleteItemMessage2<'TItem when 'TItem : comparison> = 
|    Start of 'TItem 
|    Complete of 'TItem
|    LastComplete of (AsyncReplyChannel<'TItem option>) 
|    Notify of ('TItem * string option * Async<unit>)

type LastCompleteItemAgent<'TItem when 'TItem : comparison> (?name : string) = 
    let log = createLogger <| sprintf "Eventful.LastCompleteItemAgent<%s>" typeof<'TItem>.Name

    let runCallbacks callbacks = async {
         for callback in callbacks do
            try
                do! callback
            with | e ->
                log.ErrorWithException <| lazy("Exception in notification callback", e) }

    let agent =
        let theAgent = Agent.Start(fun agent ->
            let rec loop (state : MutableLastCompleteTrackingState<'TItem>) = async {
                let! msg = agent.Receive()
                match msg with
                | Start item ->
                    match state.Start item with
                    | Some state' ->
                        return! loop state'
                    | None ->
                        log.Error <| lazy(sprintf "Item added out of order: %A" item)
                        return! loop state
                | Complete item ->
                    match state.Complete item with
                    | Some s ->
                        let (state', notificationCallbacks) = s.GetNotifications()

                        do! runCallbacks notificationCallbacks

                        return! loop state'
                    | None ->
                        log.Error <| lazy(sprintf "Item completed before started: %A" item)
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
            log.ErrorWithException <| lazy("Exception thrown by LastCompleteItemAgent", exn))
        theAgent

    member x.LastComplete () : Async<'TItem option> =
        agent.PostAndAsyncReply((fun ch -> LastComplete(ch)))

    member x.Start(item) = 
      agent.Post <| Start item

    member x.Complete(item) = 
      agent.Post(Complete item)

    member x.NotifyWhenComplete(item, tag : string option, callback :  Async<unit>) =
      agent.Post <| Notify (item, tag, callback)