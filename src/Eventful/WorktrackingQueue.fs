namespace Eventful

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorktrackingQueue<'TGroup, 'TInput, 'TWorkItem when 'TGroup : comparison>
    (
        grouping : 'TInput -> ('TWorkItem * Set<'TGroup>),
        workAction : 'TGroup -> 'TWorkItem seq -> Async<unit>,
        ?maxItems : int, 
        ?workerCount,
        ?complete : 'TInput -> Async<unit>,
        ?name : string,
        ?cancellationToken : CancellationToken
    ) =

    let _maxItems = maxItems |> getOrElse 1000
    let _workerCount = workerCount |> getOrElse 1
    let _complete = complete |> getOrElse (fun _ -> async { return () })
    let _name = name |> getOrElse "unnamed"

    let queue = new MutableOrderedGroupingBoundedQueue<'TGroup, 'TWorkItem>(_maxItems)

    let doWork (group, items) = async {
         do! workAction group items
    }

    let mutable working = true

    let workers = 
        let workAsync = async {
            while true do
                do! queue.Consume doWork
                if not working then
                    do! Async.Sleep(2000)
        }

        for i in [1.._workerCount] do
            match cancellationToken with 
            | Some token -> Async.StartAsTask(workAsync, TaskCreationOptions.None, token) |> ignore
            | None -> Async.StartAsTask(workAsync) |> ignore

    let sequenceGrouping a =
        let (item, groups) = grouping a
        groups |> Set.toSeq |> Seq.map (fun g -> (item, g))
        
    member this.StopWork () =
        working <- false

    member this.StartWork () =
        working <- true

    member this.Add (item:'TInput) =
        queue.Add (item, sequenceGrouping, _complete item)

    member this.AddWithCallback (item:'TInput, onComplete : ('TInput -> Async<unit>)) =
        queue.Add (item, sequenceGrouping, onComplete item)

    member this.AsyncComplete () =
        queue.CurrentItemsComplete ()