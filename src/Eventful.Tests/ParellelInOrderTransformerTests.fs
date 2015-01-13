namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit

module ParallelInOrderTransformerTests = 

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Transformer does not reorder operations`` () : unit = 
        let monitor = new obj()

        let rnd = new Random()

        let received = ref List.empty

        let callback i = async {
            do! Async.Sleep (rnd.Next(10))
            lock monitor (fun () ->
                received := (i::(!received ))
            )
        }

        let runItem i = async {
            do! Async.Sleep(rnd.Next(10))
            return i }

        let transformer = new ParallelInOrderTransformer<int,int>(runItem, 50, 5)

        for i in [1..100] do
            transformer.Process(i, callback)

        async {
            while(!received |> List.length < 100) do
                do! Async.Sleep(100)
        } |> (fun f -> Async.RunSynchronously(f, 5000))

        !received |> List.rev |> should equal ([1..100])

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Transformer will keep working even with an empty period`` () : unit = 
        let monitor = new obj()

        let received = ref List.empty

        let callback i = async {
            lock monitor (fun () ->
                received := (i::(!received ))
            )
        }

        let runItem i = async {
            return i }

        let transformer = new ParallelInOrderTransformer<int,int>(runItem, 50, 5)

        for i in [1..100] do
            transformer.Process(i, callback)

        async {
            while(!received |> List.length < 100) do
                do! Async.Sleep(100)
        } |> (fun f -> Async.RunSynchronously(f, 10000))

        for i in [1..100] do
            transformer.Process(i, callback)

        async {
            while(!received |> List.length < 200) do
                do! Async.Sleep(100)
        } |> (fun f -> Async.RunSynchronously(f, 5000))

        !received |> List.length |> should equal (200)

module ParallelInOrderTransformerPerfTests =
    let itemCount = 100000
    
    let eventCount = ref 0
    let result = ref 0

    let runTest eventHandler (completeTask : Task<bool>) =
        let stopwatch = System.Diagnostics.Stopwatch.StartNew()

        eventCount := 0

        for i in [1..itemCount] do
            eventHandler i |> Async.RunSynchronously

        completeTask.Wait()

        stopwatch.Stop()

        printfn "%d events in %f seconds %f" !eventCount stopwatch.Elapsed.TotalSeconds (float !eventCount / stopwatch.Elapsed.TotalSeconds)
        ()
        
    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Direct incriment`` () = 
        let eventHandler i =
            async { 
                eventCount := !eventCount + 1 
            }

        runTest eventHandler (Task.FromResult(true))

    let noWork x = async {
        return x
    }

    let onComplete (tcs : TaskCompletionSource<bool>) i =
        let newValue = System.Threading.Interlocked.Increment eventCount
            
        if newValue= itemCount - 1 then
            tcs.SetResult true
        async.Zero()
            
    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Run in parallel no work`` () = 

        let work x = async {
            return x
        }

        let transformer = new Eventful.ParallelInOrderTransformer<int,int>(noWork)

        let tcs = new TaskCompletionSource<bool>()
        let eventHandler item =
            transformer.Process (item, onComplete tcs)
            async.Zero()

        runTest eventHandler tcs.Task

    let realWork x = async {
        let sum = [1L..10000L] |> Seq.sum
        return x
    }

    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Run in parallel with work`` () = 

        let transformer = new Eventful.ParallelInOrderTransformer<int,int>(realWork)
        let tcs = new TaskCompletionSource<bool>()

        let eventHandler item =
            transformer.Process (item, onComplete tcs)
            async.Zero()

        runTest eventHandler  tcs.Task