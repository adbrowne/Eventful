namespace Eventful

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FSharpx

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
    let log = Common.Logging.LogManager.GetLogger("Eventful.WorktrackingQueue")

    let _maxItems = maxItems |> Option.getOrElse 1000
    let _workerCount = workerCount |> Option.getOrElse 1
    let _complete = complete |> Option.getOrElse (fun _ -> async { return () })
    let _name = name |> Option.getOrElse "unnamed"

    let queue = new MutableOrderedGroupingBoundedQueue<'TGroup, 'TWorkItem>(_maxItems, _name)

    let doWork (group, items) = async {
         do! workAction group items
    }

    let mutable working = true

    let workerName = (sprintf "WorktrackingQueue worker %A" name)
    let workTimeout = TimeSpan.FromSeconds(30.0)
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
                                    do! runWithTimeout workerName workTimeout ct work
                                with | e ->
                                    if log.IsDebugEnabled then
                                        log.Debug(sprintf "Work failed..retrying: %A" workerName)

                                    return! loop(count + 1)
                            else
                                log.Error(sprintf "Work failed permanently: %A" workerName)
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