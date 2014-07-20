namespace Eventful.Tests

open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open System
open Eventful

module BatchingQueueTests =
    [<Fact>]
    let ``Can batch up multiple items`` () =
        
        let queue = new BatchingQueue<int,int>(1000, 500000)

        let consumer = async {
            while true do
                let! batch = queue.Consume()
                for (i, reply) in batch do
                    reply.Reply i
        }
        for _ in [1..1000] do
         consumer |> Async.StartAsTask |> ignore

        let workflow i = async {
           return! queue.Work i
        }

        let result = 
            [1..10000]
            |> List.map (fun i -> workflow i)
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Set.ofSeq

        result |> should equal ([1..10000] |> Set.ofList)
        ()