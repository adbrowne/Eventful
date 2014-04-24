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

        let counter1 = new CounterAgent()
        let counter2 = new CounterAgent()
        let counter3 = new CounterAgent()
        let counter4 = new CounterAgent()

        let rec consumer (counter : CounterAgent)  = async {
            do! myQueue.Consume((fun (g, items) -> async {
                // Console.WriteLine(sprintf "Group: %A Items: %A ItemCount: %d" g items (items |> Seq.length))
                do! Async.Sleep 100
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
            return! consumer counter
        }

        consumer counter1 |> Async.Start
        consumer counter2 |> Async.Start
        consumer counter3 |> Async.Start
        consumer counter4 |> Async.Start

        async {
            for i in [0..10000000] do
                do! myQueue.Add(i, [1; 2; 3; 4] |> Set.ofList)
        } |> Async.Start

        async {

            printfn "About to sleep"

            do! Async.Sleep(10000) 

            let! value1 = counter1.Get()
            let! value2 = counter2.Get()
            let! value3 = counter3.Get()
            let! value4 = counter4.Get()
           
            printfn "Received %d %d %d %d total: %d" value1 value2 value3 value4 (value1 + value2 + value3 + value4) 

        } |> Async.RunSynchronously
