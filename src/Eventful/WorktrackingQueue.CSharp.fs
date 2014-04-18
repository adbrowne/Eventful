namespace Eventful.CSharp

open System
open System.Collections.Generic
open System.Runtime.InteropServices

type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        grouping : Func<'TItem, 'TGroup seq>, 
        workAction :  Func<'TGroup, 'TItem seq, System.Threading.Tasks.Task>,
        maxItems, 
        workerCount,
        complete : Func<'TItem', System.Threading.Tasks.Task>
    ) =
    let groupingfs = (fun i -> grouping.Invoke(i) |> Set.ofSeq)

    let completeFs = 
        match complete with
        | null -> (fun i -> async { return () })
        | complete ->  (fun (i:'TItem) -> async { 
                           let task = complete.Invoke(i) 
                           do! task |> Async.AwaitIAsyncResult |> Async.Ignore
                       })

    let workActionFs = (fun g i -> async { 
                        let task = workAction.Invoke(g,i) 
                        do! task |> Async.AwaitIAsyncResult |> Async.Ignore
                    })
    let queue = new Eventful.WorktrackingQueue<'TGroup, 'TItem> (groupingfs, workActionFs, maxItems, workerCount, completeFs)

    member this.Add (item:'TItem) =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        
        async {
            do! queue.Add(item)
            tcs.SetResult(true)
        } |> Async.Start

        tcs.Task

    member this.AsyncComplete () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        async {
            do! queue.AsyncComplete()
            tcs.SetResult(true)
        } |> Async.Start

        tcs.Task