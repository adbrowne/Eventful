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
    [<Trait("category", "unit")>]
    let ``Can batch up multiple items`` () =
        
        let queue = new BatchingQueue<string, int,int>(1000, 500000)

        let consumer = async {
            while true do
                let! (key, batch) = queue.Consume()
                for (i, reply) in batch do
                    reply.Reply i
        }
        for _ in [1..1000] do
         consumer |> Async.StartAsTask |> ignore

        let workflow key i = async {
           return! queue.Work key i
        }

        let result = 
            [1..10000]
            |> List.map (fun i -> [workflow "key1" i; workflow "key2" i])
            |> List.collect id
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Set.ofSeq

        result |> should equal ([1..10000] |> Set.ofList)
        ()