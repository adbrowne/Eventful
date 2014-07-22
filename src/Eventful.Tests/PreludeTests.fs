namespace Eventful.Tests

open Xunit
open FsUnit.Xunit
open Eventful
open System


module PreludeTests = 

    let log = Common.Logging.LogManager.GetLogger("Eventful.Tests.PreludeTests")

    let longTask = async { do!  Async.Sleep(100000) }
    let action = runWithTimeout "MyName" (TimeSpan.FromMilliseconds(100.0)) Async.DefaultCancellationToken longTask

    [<Fact>]
    let ``Timeout using runWithTimeout`` () : unit =
        (fun () -> action |> Async.RunSynchronously) |> should throw typeof<System.OperationCanceledException>

    [<Fact>]
    let ``Timeout inside runAsyncAsTask`` () : unit =
        let cancelled = ref false

        let action = async {
            try
                return! action
            with | e -> 
                cancelled := true
        }

        runAsyncAsTask "Task Name" Async.DefaultCancellationToken action  |> ignore

        System.Threading.Thread.Sleep(500)
        !cancelled |> should equal true