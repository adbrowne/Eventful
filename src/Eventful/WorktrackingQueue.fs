namespace Eventful

open System
open System.Collections.Generic

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        grouping : 'TItem -> Set<'TGroup>, 
        workAction : 'TGroup -> 'TItem seq -> Async<unit>,
        ?maxItems : int, 
        ?workerCount,
        ?complete : 'TItem -> Async<unit>
    ) =

    let _maxItems = maxItems |> getOrElse 1000
    let _workerCount = workerCount |> getOrElse 1
    let _complete = complete |> getOrElse (fun _ -> async { return () })

    let queue = new OrderedGroupingQueue<'TGroup, 'TItem>(_maxItems)

    let doWork (group, items) = async {
         do! workAction group items
    }

    let mutable working = true

    let workers = 
        for i in [1.._workerCount] do
            async {
                while true do
                    if working then
                        do! queue.Consume doWork
                    else
                        do! Async.Sleep(100)
            } |> Async.Start

    let grouping item =
        let groups = grouping item
        (item, groups)

    member this.StopWork () =
        working <- false

    member this.StartWork () =
        working <- true

    member this.Add (item:'TItem) =
        queue.Add (item, grouping, _complete item)

    member this.AddWithCallback (item:'TItem, onComplete : ('TItem -> Async<unit>)) =
        queue.Add (item, grouping, onComplete item)

    member this.AsyncComplete () =
        queue.CurrentItemsComplete ()