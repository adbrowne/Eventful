namespace Eventful

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

    let rec runAsyncUntilSuccess task = async {
        try
            return! task()
        with 
        | e -> return! runAsyncUntilSuccess task
    }

    let consoleLog (value:string) = System.Console.WriteLine(value)

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

    let taskLog = Common.Logging.LogManager.GetLogger("Eventful.Task")

    let runAsyncAsTask (name : string) cancellationToken action = 
        let task = Async.StartAsTask(action, System.Threading.Tasks.TaskCreationOptions.None, cancellationToken)

        let continueFunction (t : System.Threading.Tasks.Task<'a>) =
            if(t.IsFaulted) then
                taskLog.ErrorFormat("Exception thrown in task {0}", t.Exception, [|name :> obj|])
            elif(t.IsCanceled) then
                taskLog.WarnFormat("Timed out on task {0}", name)
            else
                ()

        task.ContinueWith (continueFunction) |> ignore

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

    let newAgent (name : string) (log : Common.Logging.ILog) f  =
        let agent= Agent.Start(f)
        agent.Error.Add(fun e -> log.Error(sprintf "Exception thrown by %A" name, e))
        agent