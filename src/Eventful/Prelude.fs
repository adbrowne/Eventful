namespace Eventful

type Logger internal (name : string) =
    let logger = Common.Logging.LogManager.GetLogger(name)
    member this.Debug (msg : Lazy<string>) : unit = 
        if (logger.IsDebugEnabled) then
            let message = msg.Force()
            logger.Debug(message)

    member this.DebugWithException (msg : Lazy<string * System.Exception>) : unit = 
        if (logger.IsDebugEnabled) then
            let message = msg.Force()
            let (message, exn) = msg.Force()
            logger.Debug(message, exn)

    member this.Warn (msg : Lazy<string>) : unit = 
        if (logger.IsWarnEnabled) then
            let message = msg.Force()
            logger.Warn(message)

    member this.Error (msg : Lazy<string>) : unit = 
        if (logger.IsErrorEnabled) then
            let message = msg.Force()
            logger.Error(message)

    member this.ErrorWithException (msg : Lazy<string * System.Exception>) : unit = 
        if (logger.IsErrorEnabled) then
            let (message, exn) = msg.Force()
            logger.Error(message, exn)

[<AutoOpen>]
module Prelude =
    let applyTuple2 func (a,b) = func a b
    let applyTuple3 func (a,b,c) = func a b c
    let applyTuple4 func (a,b,c,d) = func a b c d
    let applyTuple5 func (a,b,c,d,e) = func a b c d e
    let applyTuple6 func (a,b,c,d,e,f) = func a b c d e f
    let applyTuple7 func (a,b,c,d,e,f,g) = func a b c d e f g
    let applyTuple8 func (a,b,c,d,e,f,g,h) = func a b c d e f g h
    let applyTuple9 func (a,b,c,d,e,f,g,h,i) = func a b c d e f g h i

    let tupleFst2 = fst
    let tupleFst3 (a,b,c) = (a,(b,c))
    let tupleFst4 (a,b,c,d) = (a,(b,c,d))
    let tupleFst5 (a,b,c,d,e) = (a,(b,c,d,e))
    let tupleFst6 (a,b,c,d,e,f) = (a,(b,c,d,e,f))
    let tupleFst7 (a,b,c,d,e,f,g) = (a,(b,c,d,e,f,g))
    let tupleFst8 (a,b,c,d,e,f,g,i) = (a,(b,c,d,e,f,g,i))
    let tupleFst9 (a,b,c,d,e,f,g,i,j) = (a,(b,c,d,e,f,g,i,j))

    let createLogger name =
        new Logger(name)

    let rec runAsyncUntilSuccess task = async {
        try
            return! task()
        with 
        | e -> return! runAsyncUntilSuccess task
    }

    let consoleLog (value:string) = System.Console.WriteLine(value)

    let ticksIntervalToNanoSeconds startTicks endTicks =
        (endTicks - startTicks) * 100L
        |> int64

    let startNanoSecondTimer () =
        let startTicks = System.DateTime.UtcNow.Ticks
        (fun () -> 
            let endTicks = System.DateTime.UtcNow.Ticks
            ticksIntervalToNanoSeconds startTicks endTicks)

    let timeAsync (timer : Metrics.Timer) (computation) = async {
        let startTicks = System.DateTime.Now.Ticks
        let! result = computation

        let totalTicks = System.DateTime.Now.Ticks - startTicks
        let totalTimeSpan = new System.TimeSpan(totalTicks)
        timer.Record(int64 totalTimeSpan.TotalMilliseconds, Metrics.TimeUnit.Milliseconds)

        return result
    }
            
    // from: http://blogs.msdn.com/b/dsyme/archive/2009/11/08/equality-and-comparison-constraints-in-f-1-9-7.aspx
    let equalsOn f x (yobj:obj) =
        match yobj with
        | :? 'T as y -> (f x = f y)
        | _ -> false
 
    let hashOn f x =  hash (f x)
 
    let inline compareOn f x (yobj: obj) =
        match yobj with
        | :? 'T as y -> compare (f x) (f y)
        | _ -> invalidArg "yobj" "cannot compare values of different types"

    let taskLog = createLogger "Eventful.Task"

    let runAsyncAsTask (name : string) cancellationToken action = 
        let task = Async.StartAsTask(action, System.Threading.Tasks.TaskCreationOptions.None, cancellationToken)

        task

    open System
    open System.Threading
    open System.Threading.Tasks

    // adapted from 
    // http://stackoverflow.com/questions/18274986/async-catch-doesnt-work-on-operationcanceledexceptions
    let startCatchCancellation(work, cancellationToken) = 
        Async.FromContinuations(fun (cont, econt, _) ->
          // When the child is cancelled, report OperationCancelled
          // as an ordinary exception to "error continuation" rather
          // than using "cancellation continuation"
          let ccont e = econt e
          // Start the workflow using a provided cancellation token
          Async.StartWithContinuations( work, cont, econt, ccont, 
                                        ?cancellationToken=cancellationToken) )

    let runWithTimeout<'a> name (timeout : System.TimeSpan) cancellationToken (computation : 'a Async) : 'a Async =
        let tcs = new TaskCompletionSource<'a>();

        let timeout = new CancellationTokenSource(timeout);

        let combinedCancellation = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, cancellationToken)

        startCatchCancellation(computation, Some combinedCancellation.Token)

    let runWithCancellation<'a> name cancellationToken (computation : 'a Async) : 'a Async =
        let tcs = new TaskCompletionSource<'a>();

        startCatchCancellation(computation, Some cancellationToken)

    let newAgent (name : string) (log : Logger) f  =
        let agent= Agent.Start(f)
        agent.Error.Add(fun e -> log.ErrorWithException <| lazy(sprintf "Exception thrown by %A" name, e))
        agent

    // from: http://msdn.microsoft.com/en-us/library/dd233248.aspx
    let (|Integer|_|) (str: string) =
       let mutable intvalue = 0
       if System.Int32.TryParse(str, &intvalue) then Some(intvalue)
       else None

    let (|Integer64|_|) (str: string) =
       let mutable intvalue = 0L
       if System.Int64.TryParse(str, &intvalue) then Some(intvalue)
       else None

open System

/// System.Type does not implement IComparable
/// This is a convenient wrapper class that fixes 
/// that by just comparing on the AssemblyQualifiedName
type ComparableType (t : Type) =
    member this.RealType with get() : Type = t
    static member GetRealType (x : ComparableType) = x.RealType.AssemblyQualifiedName
    override x.Equals y = equalsOn ComparableType.GetRealType x y
    override x.GetHashCode() = hashOn ComparableType.GetRealType x
    interface System.IComparable with
        member x.CompareTo y = compareOn ComparableType.GetRealType x y