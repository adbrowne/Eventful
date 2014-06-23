namespace Eventful.Tests.Integration

open Xunit
open System
open FSharpx.Collections
open Raven.Client
open Eventful

type MyCountingDoc = Eventful.CsTests.MyCountingDoc

module Util = 
    let taskToAsync (task:System.Threading.Tasks.Task) =
        let wrappedTask = 
            task.ContinueWith(fun _ -> 
                if (task.IsFaulted) then
                    Some task.Exception
                else
                    None
            ) |> Async.AwaitTask
        async {
            let! result = wrappedTask
            match result with
            | Some e -> raise e
            | None -> return ()
        }

type RavenProjector (documentStore:Raven.Client.IDocumentStore) =

    let grouping = fst >> Set.singleton

    let processEvent (key:Guid) values =
        async { 
            use session = documentStore.OpenAsyncSession()

            let docKey = "MyCountingDocs/" + key.ToString()
            let! doc = session.LoadAsync<MyCountingDoc>(docKey) |> Async.AwaitTask
            let! doc = async { 
                if doc = null then
                    let newDoc = new MyCountingDoc()
                    do! session.StoreAsync(newDoc, docKey) |> Util.taskToAsync
                    return newDoc
                else
                    return doc
            }

            for (_, value) in values do
                let isEven = doc.Count % 2 = 0
                doc.Count <- doc.Count + 1
                if isEven then
                    doc.Value <- doc.Value + value
                else
                    doc.Value <- doc.Value - value
            do! session.SaveChangesAsync() |> Util.taskToAsync
        }

    let queue = new WorktrackingQueue<Guid, Guid * int>(fst >> Set.singleton, processEvent, 10000, 10);

    member x.Enqueue key value =
       queue.Add (key,value)
   
    member x.WaitAll = queue.AsyncComplete

module RavenProjectorTests = 

    [<Fact>]
    let ``Pump many events at Raven`` () : unit =
        let documentStore = new Raven.Client.Document.DocumentStore()
        documentStore.Url <- "http://localhost:8080"
        documentStore.DefaultDatabase <- "tenancy-blue"
        documentStore.Initialize() |> ignore

        let projector = new RavenProjector(documentStore :> Raven.Client.IDocumentStore)

        let values = [1..100]
        let streams = [for i in 1 .. 10000 -> Guid.NewGuid()]

        let streamValues = 
            streams
            |> Seq.map (fun x -> (x,values))
            |> Map.ofSeq

        let rnd = new Random(1024)

        let rec generateStream (remainingStreams, remainingValues:Map<Guid, int list>) = 
            match remainingStreams with
            | [] -> None
            | _ ->
                let index = rnd.Next(0, remainingStreams.Length - 1)
                let blah = List.nth
                let key =  List.nth remainingStreams index
                let values = remainingValues |> Map.find key

                match values with
                | [] -> failwith ("Empty sequence should not happen")
                | [x] -> 
                    let beforeIndex = remainingStreams |> List.take index
                    let afterIndex = remainingStreams |> List.skip (index + 1) 
                    let remainingStreams' = (beforeIndex @ afterIndex)
                    let remainingValues' = (remainingValues |> Map.remove key)
                    let nextValue = (key,x)
                    let remaining = (remainingStreams', remainingValues')
                    Some (nextValue, remaining)
                | x::xs ->
                    let remainingValues' = (remainingValues |> Map.add key xs)
                    let nextValue = (key,x)
                    let remaining = (remainingStreams, remainingValues')
                    Some (nextValue, remaining)

        let myEvents = (streams, streamValues) |> Seq.unfold generateStream

        try
            myEvents |> Seq.length |> printfn "%d"
        with
        | e -> printfn "%A" e

        async {
            for (key,value) in myEvents do
                do! projector.Enqueue key value
            do! projector.WaitAll()
        } |> Async.RunSynchronously 

        ()