namespace Eventful

open System
open FSharpx.Collections
open FSharpx

type SortedSet<'a> = System.Collections.Generic.SortedSet<'a>

[<CustomEquality; CustomComparison>]
type NotificationItem<'TItem when 'TItem : comparison> = {
    Item : 'TItem
    Unique : Guid // each item in the set must be unique
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

type LastCompleteItemAgent2<'TItem when 'TItem : comparison> () = 
    let log = Common.Logging.LogManager.GetLogger(typeof<LastCompleteItemAgent2<_>>)
    let started = new System.Collections.Generic.SortedSet<'TItem>()
    let completed = new System.Collections.Generic.SortedSet<'TItem>()
    let notifications = new System.Collections.Generic.SortedSet<NotificationItem<'TItem>>()

    let mutable nextToComplete = None
    let mutable currentLastComplete = None

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
        | Some ({ Item = item; Callback = callback } as value) ->
            if(item <= lastComplete) then
                do! callback
                notifications.Remove value |> ignore
                return! checkNotifications lastComplete 
            else
                return ()
        | None -> return ()
    }

    let agent = Agent.Start(fun agent ->
        let rec loop state = async {
            let! msg = agent.Receive()
            match msg with
            | Start (item, reply) ->
                reply.Reply()
                started.Add(item) |> ignore
                
                match nextToComplete with
                | Some next when next > item ->
                    nextToComplete <- Some item
                | None ->
                    nextToComplete <- Some item
                | _ -> ()

                return! loop state
            | Complete item ->
                completed.Add(item) |> ignore

                if Some item = nextToComplete then
                    let lastComplete' = removeMatchingHeads started completed

                    currentLastComplete <-
                        match lastComplete' with
                        | Some x -> Some x
                        | None -> currentLastComplete
                    
                    match currentLastComplete with
                    | Some x -> do! checkNotifications x
                    | None -> ()

                    nextToComplete <- (started |> Seq.tryHead)

                return! loop state
            | LastComplete reply ->
                reply.Reply(currentLastComplete)
                return! loop ()
            | Notify (item, callback) ->
                notifications.Add({ Item = item; Unique = Guid.NewGuid(); Callback = callback}) |> ignore

                match currentLastComplete with
                | Some x -> do! checkNotifications x
                | None -> ()

                return! loop ()
        }

        loop ()
    )

    member x.LastComplete () : Async<'TItem option> =
        agent.PostAndAsyncReply((fun ch -> LastComplete(ch)))

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)

    member x.NotifyWhenComplete(item, callback :  Async<unit>) =
      agent.Post <| Notify (item, callback)