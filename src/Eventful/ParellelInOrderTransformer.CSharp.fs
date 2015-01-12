namespace Eventful.CSharp

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading.Tasks

type ParallelInOrderTransformer<'TInput,'TOutput>
    (
        work : System.Func<'TInput, Task<'TOutput>>, 
        maxItems : int, 
        workers : int
    ) =

    let transformer = new Eventful.ParallelInOrderTransformer<'TInput, 'TOutput>((fun i -> work.Invoke(i) |> Async.AwaitTask), maxItems, workers)

    member x.Process(input: 'TInput, onComplete : System.Func<'TOutput, Task>) : unit =
        transformer.Process(input, (fun o -> onComplete.Invoke(o) |> Async.AwaitIAsyncResult |> Async.Ignore))