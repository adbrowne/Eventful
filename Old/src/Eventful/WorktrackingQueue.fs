namespace Eventful

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FSharpx

type IWorktrackingQueue<'TGroup,'TInput, 'TWorkItem> = 
    [<CLIEvent>]
    abstract member QueueFullEvent : IEvent<unit>
    abstract member Add : 'TInput -> Async<unit>
    abstract member AsyncComplete : unit -> Async<unit>
    abstract member StartWork : unit -> unit

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorktrackingQueue<'TGroup, 'TInput, 'TWorkItem>
    (
        grouping : 'TInput -> seq<'TWorkItem * 'TGroup>,
        workAction : 'TGroup -> 'TWorkItem seq -> Async<unit>,
        ?maxItems : int, 
        ?workerCount,
        ?complete : 'TInput -> Async<unit>,
        ?name : string,
        ?cancellationToken : CancellationToken,
        ?groupComparer : System.Collections.Generic.IComparer<'TGroup>,
        ?runImmediately : bool,
        ?workTimeout : TimeSpan option
    ) =
    let log = createLogger "Eventful.WorktrackingQueue"

    let _maxItems = maxItems |> Option.getOrElse 1000
    let _workerCount = workerCount |> Option.getOrElse 1
    let _complete = complete |> Option.getOrElse (fun _ -> async { return () })
    let _name = name |> Option.getOrElse "unnamed"

    let queue = 
        new MutableOrderedGroupingBoundedQueue<'TGroup, 'TWorkItem>(_maxItems, _name, ?groupComparer = groupComparer)

    let doWork (group, items) = async {
         do! workAction group items
    }

    let mutable working = 
        match runImmediately with
        | Some v -> v
        | None -> true

    let workerName = (sprintf "WorktrackingQueue worker %A" name)
    let workers = 
        let workAsync = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                if not working then
                    do! Async.Sleep(2000)
                else
                    let! work = queue.Consume doWork

                    let maxAttempts = 10
                    let rec loop count = 
                        async {
                            if count < maxAttempts then
                                try
                                    match workTimeout with
                                    | Some (Some timeout) ->
                                        do! runWithTimeout workerName timeout ct work
                                    | _ ->
                                        do! runWithCancellation workerName ct work
                                with | e ->
                                    log.DebugWithException <| lazy (sprintf "Work failed..retrying: %A" workerName, e)

                                    return! loop(count + 1)
                            else
                                log.Error <| lazy (sprintf "Work failed permanently: %A" workerName)
                                ()
                        }
                    do! loop 0
        }

        let cancellationToken =
            match cancellationToken with 
            | Some token -> token
            | None -> Async.DefaultCancellationToken
            
        for i in [1.._workerCount] do
            runAsyncAsTask workerName cancellationToken workAsync |> ignore

    /// fired each time a full queue is detected
    [<CLIEvent>]
    member this.QueueFullEvent = queue.QueueFullEvent

    member this.StopWork () =
        working <- false

    member this.StartWork () =
        working <- true

    member this.Add (item:'TInput) =
        queue.Add (item, grouping, _complete item)

    member this.AddWithCallback (item:'TInput, onComplete : ('TInput -> Async<unit>)) =
        queue.Add (item, grouping, onComplete item)

    member this.AsyncComplete () =
        queue.CurrentItemsComplete ()

    interface IWorktrackingQueue<'TGroup, 'TInput, 'TWorkItem> with
        member x.Add input = x.Add input
        member x.AsyncComplete () = x.AsyncComplete ()
        member x.StartWork () = x.StartWork ()
        [<CLIEvent>]
        member x.QueueFullEvent = x.QueueFullEvent
