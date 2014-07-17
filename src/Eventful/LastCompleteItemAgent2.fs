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

type LastCompleteItemMessage2<'TItem when 'TItem : comparison> = 
|    Start of ('TItem * AsyncReplyChannel<unit>) 
|    Complete of 'TItem
|    LastComplete of (AsyncReplyChannel<'TItem option>) 
|    Notify of ('TItem * string option * Async<unit>)

type LastCompleteItemAgent2<'TItem when 'TItem : comparison> (?name : string) = 
    let log = Common.Logging.LogManager.GetLogger(typeof<LastCompleteItemAgent2<_>>)
    let started = new System.Collections.Generic.SortedSet<'TItem>()
    let completed = new System.Collections.Generic.SortedSet<'TItem>()
    let notifications = new System.Collections.Generic.SortedSet<NotificationItem<'TItem>>()

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

    let rec checkNotifications lastComplete = async {
        match (notifications |> Seq.tryHead) with
        | Some ({ Item = item; Callback = callback; Tag = tag; Unique = unique } as value) ->
            if(item <= lastComplete) then
                do! callback
                notifications.Remove value |> ignore
                return! checkNotifications lastComplete 
            else
                return ()
        | None -> return ()
    }

    let agent =
        let theAgent =  Agent.Start(fun agent ->
            let rec loop state = async {
                let! msg = agent.Receive()
                match msg with
                | Start (item, reply) ->
                    reply.Reply()

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
                    else
                        log.ErrorFormat("Item added out of order: {0}",item)

                    return! loop state
                | Complete item ->
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

                            match currentLastComplete with
                            | Some x -> do! checkNotifications x
                            | None -> ()
                        else
                            log.ErrorFormat("Item completed before started: {0}",item)
                    else ()

                    return! loop state
                | LastComplete reply ->
                    reply.Reply(currentLastComplete)
                    return! loop ()
                | Notify (item, tag, callback) ->
                    let uniqueId = Guid.NewGuid()

                    let added = notifications.Add({ Item = item; Unique = uniqueId; Tag = tag; Callback = callback}) 

                    match currentLastComplete with
                    | Some x -> do! checkNotifications x
                    | None -> ()

                    return! loop ()
            }

            loop ()
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