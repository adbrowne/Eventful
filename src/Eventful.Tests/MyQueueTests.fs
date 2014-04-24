namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
module MyQueueTests = 

    [<Fact>]
    let ``Can do something`` () : unit = 
        let myQueue = new MyQueue<int, int>()

        let counter = new CounterAgent()

        let rec consumer () = async {
            do! myQueue.Consume((fun (g, items) -> async {
                // Console.WriteLine(sprintf "Group: %A Items: %A ItemCount: %d" g items (items |> Seq.length))
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
            return! consumer()
        }

        consumer() |> Async.Start

        async {
            for i in [0..10000000] do
                do! myQueue.Add(i, [1; 2; 3; 4] |> Set.ofList)
        } |> Async.Start

        async {

            printfn "About to sleep"

            do! Async.Sleep(10000) 

            let! value = counter.Get()
           
            printfn "Received %d" value 

        } |> Async.RunSynchronously
