namespace Eventful.CSharp

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading.Tasks

type ParallelInOrderTransformer<'TInput,'TOutput>
    (
        work : System.Func<'TInput, 'TOutput>, 
        maxItems : int, 
        workers : int
    ) =

    let transformer = new Eventful.ParallelInOrderTransformer<'TInput, 'TOutput>((fun i -> work.Invoke(i)), maxItems, workers)

    member x.Process(input: 'TInput, onComplete : System.Action<'TOutput>) : unit =
        transformer.Process(input, (fun o -> onComplete.Invoke(o)))

    interface IDisposable with
         member this.Dispose() = (transformer :> IDisposable).Dispose()