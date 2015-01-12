namespace Eventful.Tests.Integration

open Xunit
open System
open Eventful
open Eventful.EventStore
open System.Threading.Tasks
open EventStore.ClientAPI
open FSharpx

module EventStorePlay =
    let connection () =
        InMemoryEventStoreRunner.connectToEventStore 2114
    
    [<Fact>]
    [<Trait("category", "blah")>]
    let ``Run through EventStore`` () = 
        let eventCount = ref 0
        let eventHandler a b =
            async { () }
//                eventCount := !eventCount + 1
//            }

        async {
            let conn = connection()
            let client = new Client(conn)

            let tcs = new TaskCompletionSource<bool>()

            let onLive () = 
                tcs.SetResult(true)
                ()

            let stopwatch = System.Diagnostics.Stopwatch.StartNew()
            client.subscribe (Some EventStore.ClientAPI.Position.Start) eventHandler onLive |> ignore

            do! tcs.Task |> Async.AwaitTask |> Async.Ignore

            stopwatch.Stop()

            printfn "%d events in %f seconds %f" !eventCount stopwatch.Elapsed.TotalSeconds (float !eventCount / stopwatch.Elapsed.TotalSeconds)
            ()
        } |> Async.StartAsTask

    [<Fact>]
    [<Trait("category", "blah")>]
    let ``Run in parallel`` () = 

        let work x = async {
            let sum = [1L..100000L] |> Seq.sum
            return x
        }
        let transformer = new Eventful.ParallelInOrderTransformer<ResolvedEvent,ResolvedEvent>(work)

        let eventCount = ref 0
        let eventHandler _ (e : ResolvedEvent) =
            transformer.Process (e, fun _ -> eventCount := !eventCount + 1; async.Zero())
            async.Zero()

        async {
            let conn = connection()
            let client = new Client(conn)

            let tcs = new TaskCompletionSource<bool>()

            let onLive () = 
                tcs.SetResult(true)
                ()

            let stopwatch = System.Diagnostics.Stopwatch.StartNew()
            client.subscribe (Some EventStore.ClientAPI.Position.Start) eventHandler onLive |> ignore

            do! tcs.Task |> Async.AwaitTask |> Async.Ignore

            stopwatch.Stop()

            printfn "%d events in %f seconds %f" !eventCount stopwatch.Elapsed.TotalSeconds (float !eventCount / stopwatch.Elapsed.TotalSeconds)
            ()
        } |> Async.StartAsTask
