namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit

module ParallelInOrderTransformerTests = 

    [<Fact>]
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