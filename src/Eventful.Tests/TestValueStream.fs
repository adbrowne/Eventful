namespace Eventful.Tests

open System
open FSharpx.Collections
open FSharpx
open Eventful

type EventContext = {
    Tenancy : string
    Position : EventPosition
}

module TestEventStream =
    let sequentialNumbers streamCount valuesPerStreamCount = 

        let values = [1..valuesPerStreamCount]
        let streams = [for i in 1 .. streamCount -> Guid.NewGuid()]

        let streamValues = 
            streams
            |> Seq.map (fun x -> (x,values))
            |> Map.ofSeq

        let rnd = new Random(1024)

        let rec generateStream (eventPosition, remainingStreams, remainingValues:Map<Guid, int list>) = 
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
                    let nextValue = (eventPosition, key,x)
                    let remaining = (eventPosition + 1, remainingStreams', remainingValues')
                    Some (nextValue, remaining)
                | x::xs ->
                    let remainingValues' = (remainingValues |> Map.add key xs)
                    let nextValue = (eventPosition, key,x)
                    let remaining = (eventPosition + 1, remainingStreams, remainingValues')
                    Some (nextValue, remaining)

        let myEvents = 
            (0, streams, streamValues) 
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