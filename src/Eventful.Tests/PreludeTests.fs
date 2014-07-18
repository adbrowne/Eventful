namespace Eventful.Tests

open Xunit
open FsUnit.Xunit
open Eventful
open System

module PreludeTests = 

    let log = Common.Logging.LogManager.GetLogger("Eventful.Tests.PreludeTests")

    [<Fact>]
    let ``blah`` () : unit =
        let longTask = async { do!  Async.Sleep(100000) }

        let action = async {
            try
                do! runWithTimeout "MyName" (TimeSpan.FromSeconds(3.0)) longTask
            with | e -> log.Error(e)
        } 

        runAsyncAsTask "Blah" Async.DefaultCancellationToken action

        System.Threading.Thread.Sleep(10000)
        //(fun () ->  |> Async.RunSynchronously) |> should throw typeof<System.OperationCanceledException>