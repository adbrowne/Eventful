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

    let workers = 
        for i in [1.._workerCount] do
            async {
                while true do
                    do! queue.Consume doWork
            } |> Async.Start

    let grouping item =
        let groups = grouping item
        (item, groups)

    member this.Add (item:'TItem) =
        queue.Add (item, grouping, _complete item)

    member this.AddWithCallback (item:'TItem, onComplete : ('TItem -> Async<unit>)) =
        queue.Add (item, grouping, onComplete item)

    member this.AsyncComplete () =
        queue.CurrentItemsComplete ()