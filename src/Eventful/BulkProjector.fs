namespace Eventful

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Metrics

open FSharpx

type IProjector<'TMessage, 'TContext, 'TAction> = 
    abstract member Compare : obj -> obj -> int
    abstract member MatchingKeys : 'TMessage -> obj seq
    abstract member ProcessEvents : 'TContext -> obj -> 'TMessage seq -> Async<'TAction seq * Async<unit>>

type BulkProjectorWithContext<'TMessage, 'TAction> =
    { MatchingKeys : 'TMessage -> obj seq
      ProcessEvents : obj -> 'TMessage seq -> Async<'TAction seq * Async<unit>>
      Compare : obj -> obj -> int }

type Projector<'TKey, 'TMessage, 'TContext, 'TAction when 'TKey : comparison> =
    { MatchingKeys : 'TMessage -> 'TKey seq
      ProcessEvents : 'TContext -> 'TKey -> 'TMessage seq -> Async<'TAction seq * Async<unit>> }
    interface IProjector<'TMessage, 'TContext, 'TAction> with
        member x.Compare a b =
            let a = a :?> 'TKey
            let b = b :?> 'TKey
            compare a b
            
        member x.MatchingKeys message =
            x.MatchingKeys message
            |> Seq.map (fun x -> x :> obj)

        member x.ProcessEvents context key events =
            let key = key :?> 'TKey
            x.ProcessEvents context key events

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module BulkProjector =
    let allMatchingKeys projectors event =
        projectors
        |> Seq.mapi tuple2
        |> Seq.collect (fun (projectorIndex, projector : BulkProjectorWithContext<_,_>) ->
            projector.MatchingKeys event
            |> Seq.map (fun key -> (event, (key, projectorIndex))))

    let projectorsWithContext projectors context =
        projectors
        |> Seq.map (fun (projector : IProjector<_, _, _>) ->
            { BulkProjectorWithContext.MatchingKeys = projector.MatchingKeys
              ProcessEvents = projector.ProcessEvents context
              Compare = projector.Compare })

    let funcTaskToAsync (func : Func<Task>) =
        if func = null then async.Zero()
        else async { return! func.Invoke() |> voidTaskAsAsync }

    let createProjector<'TKey, 'TMessage, 'TContext, 'TAction when 'TKey : comparison> (matchingKeys : Func<'TMessage, 'TKey seq>) (processEvents : Func<'TContext, 'TKey, 'TMessage seq, Task<'TAction seq * Func<Task>>>) =
        { MatchingKeys = fun message -> matchingKeys.Invoke(message)
          ProcessEvents = fun context key messages ->
            processEvents.Invoke(context, key, messages)
            |> Async.AwaitTask
            |> Async.map (fun (actions, onComplete) -> (actions, funcTaskToAsync onComplete)) }

type BulkProjector<'TMessage, 'TAction when 'TMessage :> IBulkMessage>
    (
        projectorName : string,
        projectors : BulkProjectorWithContext<'TMessage, 'TAction> seq,
        executor : 'TAction seq -> Async<Choice<unit, exn>>,
        cancellationToken : CancellationToken,
        onEventComplete : 'TMessage -> Async<unit>,
        getPersistedPosition : Async<EventPosition option>,
        writeUpdatedPosition : EventPosition -> Async<bool>,
        positionWritePeriod : TimeSpan,
        maxEventQueueSize : int,
        eventWorkers : int,
        workTimeout : TimeSpan option
    ) =
    let projectors = projectors |> Seq.toArray

    let log = createLogger "Eventful.BulkProjector"

    let completeItemsTracker = Metric.Meter(sprintf "EventsComplete %s" projectorName, Unit.Items)
    let processingExceptions = Metric.Meter(sprintf "ProcessingExceptions %s" projectorName, Unit.Items)

    let tryEvent key projectorIndex events =
        async {
            let projector = projectors.[projectorIndex]
            let! actions, onComplete = projector.ProcessEvents key events
            let! result = executor actions
            return (result, onComplete)
        }
        
    let processEvent (key, projectorIndex) values = async {
        let cachedValues = values |> Seq.cache
        let maxAttempts = 10
        let rec loop count exceptions = async {
            if count < maxAttempts then
                try
                    let! attempt, onComplete = tryEvent key projectorIndex cachedValues
                    match attempt with
                    | Choice1Of2 _ ->
                        do! onComplete
                        return ()
                    | Choice2Of2 ex ->
                        return! loop (count + 1) (ex::exceptions)
                with | ex ->
                    return! loop(count + 1) (ex::exceptions)
            else
                processingExceptions.Mark()
                log.Error <| lazy(sprintf "Processing failed permanently for %s %A. Exceptions to follow." projectorName key)
                for ex in exceptions do
                    log.ErrorWithException <| lazy(sprintf "Processing failed permanently for %s %A" projectorName key, ex)
                ()
        }
        do! loop 0 []
    }
    
    let tracker = 
        let t = new LastCompleteItemAgent<EventPosition>(name = projectorName)

        async {
            let! persistedPosition = getPersistedPosition

            let position = 
                persistedPosition |> Option.getOrElse EventPosition.Start

            t.Start position
            t.Complete position
                
        } |> Async.RunSynchronously

        t

    let eventComplete (event : 'TMessage) =
        seq {
            let position = event.GlobalPosition
            match position with
            | Some position ->
                yield async {
                    tracker.Complete(position)
                    completeItemsTracker.Mark(1L)
                }
            | None -> ()

            yield onEventComplete event
        }
        |> Async.Parallel
        |> Async.Ignore

    let keyAndIndexComparer =
        { new IComparer<'TKey * int> with
            member this.Compare((keyA, indexA), (keyB, indexB)) =
                let indexComparison = compare indexA indexB
                
                if indexComparison = 0 then
                    let projector = projectors.[indexA]
                    projector.Compare (keyA :> obj) (keyB :> obj)
                else
                    indexComparison
             }

    let queue = 
        new WorktrackingQueue<_,_,_>(
            BulkProjector.allMatchingKeys projectors, 
            processEvent, 
            maxEventQueueSize, 
            eventWorkers, 
            eventComplete, 
            name = projectorName + " processing", 
            cancellationToken = cancellationToken, 
            groupComparer = keyAndIndexComparer, 
            runImmediately = false,
            workTimeout = workTimeout)
        :> IWorktrackingQueue<_,_,_>

    let mutable lastPositionWritten : Option<EventPosition> = None

    /// fired each time a full queue is detected
    [<CLIEvent>]
    member this.QueueFullEvent = queue.QueueFullEvent

    member x.LastComplete () = tracker.LastComplete()

    // todo ensure this is idempotent
    // at the moment it can be called multiple times
    member x.StartPersistingPosition () = 
        let positionWritePeriodMilliseconds =
            positionWritePeriod.TotalMilliseconds |> int

        let rec loop () =  async {
            do! Async.Sleep(positionWritePeriodMilliseconds)

            let! position = x.LastComplete()

            let! positionWasUpdated =
                match (position, lastPositionWritten) with
                | Some position, None -> 
                    writeUpdatedPosition position
                | Some position, Some lastPosition
                    when position <> lastPosition ->
                    writeUpdatedPosition position
                | _ -> async { return false }

            if positionWasUpdated then
                lastPositionWritten <- position

            let! ct = Async.CancellationToken
            if(ct.IsCancellationRequested) then
                return ()
            else
                return! loop ()
        }
            
        let taskName = sprintf "Persist Position %s" projectorName
        let task = runAsyncAsTask taskName cancellationToken <| loop ()
        
        ()

    member x.ProjectorName = projectorName

    member x.Enqueue (message : 'TMessage) =
        async {
            match message.GlobalPosition with
            | Some position -> 
                tracker.Start position
            | None -> ()

            do! queue.Add message
        }
   
    member x.WaitAll = queue.AsyncComplete

    member x.StartWork () = 
        // writeQueue.StartWork()
        queue.StartWork()
    
