namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
#nowarn "40" // disable recursive definition warning
module BoundedWorkQueueTests = 

    [<Fact(Skip = "Slow performance test")>]
    let ``Can enqueue and dequeue an item`` () : unit = 
        let rec boundedWorkQueue = new BoundedWorkQueue<int>(1000, workQueued)

        and workQueued item = async { boundedWorkQueue.WorkComplete(1) }

        for i in [0..1000000] do
            boundedWorkQueue.QueueWork i |> Async.RunSynchronously