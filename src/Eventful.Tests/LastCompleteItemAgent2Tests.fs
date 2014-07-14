namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
open FsCheck
open FsCheck.Prop
open FsCheck.Gen
open FsCheck.Xunit
open Eventful.Tests

module LastCompleteItemAgent2Tests = 
    let log = Common.Logging.LogManager.GetLogger(typeof<LastCompleteItemAgent2<_>>)

    [<Fact>]
    let ``Can have two callbacks for the one item`` () : unit =  
        let lastCompleteTracker = new LastCompleteItemAgent2<int64>(Guid.NewGuid().ToString())

        let monitor = new obj()

        let called = ref 0

        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        let callback = async {
            lock monitor (fun () -> 
                let result = System.Threading.Interlocked.Add(called, 1) 
                if(!called = 2) then tcs.SetResult(true)
                ()
            )
        }

        async {
            lastCompleteTracker.NotifyWhenComplete(1L, Some "My Thing", callback) 
            do! Async.Sleep(100)
            do! lastCompleteTracker.Start(1L)
            do! Async.Sleep(100)
            lastCompleteTracker.NotifyWhenComplete(1L, Some "My Thing2", callback) 
            do! Async.Sleep(100)
            lastCompleteTracker.Complete(1L)
            do! Async.Sleep(100)
            do! tcs.Task |> Async.AwaitTask |> Async.Ignore
            ()
        } |> (fun f -> Async.RunSynchronously(f, 1000))

        !called |> should equal 2