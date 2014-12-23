namespace Eventful.Tests

open Xunit
open FSharpx

module ActorPerformanceTests =  
    type Agent<'T> = MailboxProcessor<'T>

    let oneMillion = 1000000

    [<Fact>]
    let ``Post 1 million items`` () =    
        let theAgent = MailboxProcessor.Start(fun agent -> 
            let rec loop () = async {
                let! msg = agent.Receive()
                return! loop () }
            loop ())

        let items = List.init oneMillion id

        let sw = System.Diagnostics.Stopwatch.StartNew()
        for item in items do
            theAgent.Post(item)
        sw.Stop()

        printfn "Time elapsed %dms" sw.ElapsedMilliseconds

    [<Fact>]
    let ``1 million items to actually be processed`` () =    
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        let theAgent = MailboxProcessor.Start(fun agent -> 
            let rec loop count = async {
                if count = oneMillion then
                    tcs.SetResult true
                else
                    ()

                let! msg = agent.Receive()
                return! loop (count + 1) }
            loop 0)

        let items = List.init oneMillion id

        let sw = System.Diagnostics.Stopwatch.StartNew()
        for item in items do
            theAgent.Post(item)

        tcs.Task.Wait()

        sw.Stop()

        printfn "Time elapsed %dms" sw.ElapsedMilliseconds

    type Message = 
    | Enqueue of int
    | Read of AsyncReplyChannel<int>

    [<Fact>]
    let ``Enqueue and read 1 million items`` () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        let theAgent = MailboxProcessor.Start(fun agent -> 
            let rec empty () = agent.Scan(fun msg ->
                match msg with
                | Enqueue value -> Some (loop [value])
                | _ -> None
                )
            and loop items = async {
                let! msg = agent.Receive()
                return! 
                    match msg with
                    | Enqueue value -> loop (value::items)
                    | Read r ->
                        match items with
                        | x::xs -> 
                            r.Reply x
                            match xs with
                            | [] -> empty ()
                            | _ -> loop xs
                        | _ -> Async.returnM ()
            }
            empty ())

        let rec worker count = async {
            if count = oneMillion then
                tcs.SetResult(true)
            else
                let! item = theAgent.PostAndAsyncReply(fun ch -> Read ch)
                return! worker (count + 1)
        }

        let workerTask = worker 0 |> Async.StartAsTask

        let items = List.init oneMillion id

        let sw = System.Diagnostics.Stopwatch.StartNew()
        for item in items do
            theAgent.Post(Enqueue item)
        sw.Stop()
        let enqueueTime = sw.ElapsedMilliseconds

        sw.Reset()
        sw.Start()
        tcs.Task.Wait()
        sw.Stop()

        printfn "Enqueue: %dms, Complete work after Enqueue: %dms" enqueueTime sw.ElapsedMilliseconds

    [<Fact>]
    let ``Enqueue and read 1 million items concurrent queue`` () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        let queue = System.Collections.Concurrent.ConcurrentQueue<int>()

        let theAgent = MailboxProcessor.Start(fun agent -> 
            let rec loop () = async {
                let! msg = agent.Receive()
                queue.Enqueue msg
                return! loop () }
            loop ())

        let rec worker count = 
            if count = oneMillion then
                tcs.SetResult true
            else 
                ()

            let (success, item) = queue.TryDequeue()
            if success then
                worker (count + 1)
            else
                worker count

        let workerTask = async { worker 0 } |> Async.StartAsTask

        let items = List.init oneMillion id

        let sw = System.Diagnostics.Stopwatch.StartNew()
        for item in items do
            theAgent.Post(item)
        sw.Stop()
        let enqueueTime = sw.ElapsedMilliseconds

        sw.Reset()
        sw.Start()
        tcs.Task.Wait()
        sw.Stop()

        printfn "Enqueue: %dms, Complete work after Enqueue: %dms" enqueueTime sw.ElapsedMilliseconds