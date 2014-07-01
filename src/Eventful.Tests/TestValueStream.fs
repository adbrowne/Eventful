namespace Eventful.Tests

open System
open FSharpx.Collections
open FSharpx
open Eventful

type EventContext = {
    Tenancy : string
    Position : EventPosition
}

type Dictionary<'Key,'Value> = System.Collections.Generic.Dictionary<'Key,'Value>

module TestEventStream =
    let sequentialNumbers streamCount valuesPerStreamCount = 

        let values = [1..valuesPerStreamCount]
        let streams = [for i in 1 .. streamCount -> Guid.NewGuid()]

        let streamValues = 
            streams
            |> Seq.map (fun x -> (x,values))

        let dictionary = new Dictionary<Guid, int list>()
        for (key, value) in streamValues do
            dictionary.Add(key, value)

        let rnd = new Random(1024)

        let streamValueOrdering =
            streams
            |> Seq.map(fun s -> Seq.repeat s |> Seq.take 100)
            |> Seq.collect id
            |> Seq.sortBy(fun x -> rnd.Next(100000))
            |> LazyList.ofSeq

        let rec generateStream (eventPosition, streamValueOrdering, remainingValues:Dictionary<Guid, int list>) = 
            match streamValueOrdering with
            | LazyList.Nil -> None
            | LazyList.Cons(key,t) ->
                let values = remainingValues.Item(key)
                let x = values |> Seq.head
                let nextValue = (eventPosition, key,x)
                remainingValues.[key] <- (values |> List.tail)
                let remaining = (eventPosition + 1, t, remainingValues)
                Some (nextValue, remaining)

        let myEvents = 
            (0, streamValueOrdering, dictionary) 
            |> Seq.unfold generateStream
            |> Seq.map (fun (eventPosition, key, value) ->
                {
                    Event = (key, value)
                    Context = { 
                                Tenancy = "tenancy-blue";
                                Position = { Commit = int64 eventPosition; Prepare = int64 eventPosition } 
                              }
                    StreamId = key.ToString()
                    EventNumber = 0
                }
            )

        myEvents