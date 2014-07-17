namespace Eventful.Tests

open Xunit
open FsUnit.Xunit
open Eventful

module PreludeTests = 
    [<Fact>]
    let ``blah`` () : unit =
        let longTask = Async.Sleep(10000)

        (fun () -> runWithTimeout "MyName" 3 longTask |> Async.RunSynchronously) |> should throw typeof<System.OperationCanceledException>