namespace Eventful.CSharp

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open Eventful

type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        grouping : Func<'TItem, 'TGroup seq>, 
        workAction :  Func<'TGroup, 'TItem seq, System.Threading.Tasks.Task>,
        maxItems, 
        workerCount,
        complete : Func<'TItem, System.Threading.Tasks.Task>
    ) =
    
    let sequenceGrouping item =
        grouping.Invoke(item)
        |> Seq.map (fun group -> (item, group))

    let completeFs = 
        match complete with
        | null -> (fun i -> async { return () })
        | complete ->  (fun (i : 'TItem) -> async { 
                           let task = complete.Invoke(i) 
                           do! task |> Async.AwaitIAsyncResult |> Async.Ignore
                       })

    let workActionFs = (fun g i -> async { 
                        let task = workAction.Invoke(g,i) 
                        do! task |> Async.AwaitIAsyncResult |> Async.Ignore
                    })

    let queue = new Eventful.WorktrackingQueue<'TGroup, 'TItem, 'TItem> (sequenceGrouping, workActionFs, maxItems, workerCount, completeFs)

    member this.Add (item:'TItem) =
        runAsyncAsTask "Work Tracking Queue Add" Async.DefaultCancellationToken <| queue.Add(item)

    member this.AsyncComplete () =
        runAsyncAsTask "Work Tracking Queue AsyncComplete" Async.DefaultCancellationToken <| queue.AsyncComplete()
