namespace Eventful.Tests

open System
open FSharpx.Collections
open FSharpx
open Eventful

type EventContext = {
    Tenancy : string
    Position : EventPosition
}

type IEvent = interface end

type Dictionary<'Key,'Value> = System.Collections.Generic.Dictionary<'Key,'Value>

type SubscriberEvent = 
    { Event : obj
      Context : EventContext
      StreamId : string
      EventNumber: int }
    interface IBulkMessage with
        member x.GlobalPosition = Some x.Context.Position
        member x.EventType = x.Event.GetType()

module TestEventStream =
    let sequentialValues streamCount valuesPerStreamCount =
        let streamValues = [1..valuesPerStreamCount]
        let streams = [for i in 1 .. streamCount -> Guid.NewGuid()]

        let dictionary = new Dictionary<Guid, int list>()

        let rnd = new Random(1024)

        let streamValueOrdering =
            streams
            |> Seq.map(fun s -> Seq.repeat s |> Seq.take valuesPerStreamCount)
            |> Seq.collect id
            |> Seq.sortBy(fun x -> rnd.Next(100000))
            |> LazyList.ofSeq

        let rec generateStream (eventPosition, streamValueOrdering, remainingValues:Dictionary<Guid, int list>) = 
            match streamValueOrdering with
            | LazyList.Nil -> None
            | LazyList.Cons(key,t) ->
                let values = 
                    if(remainingValues.ContainsKey(key)) then
                        remainingValues.Item(key)
                    else
                        streamValues
                let x = values |> Seq.head
                let nextValue = (eventPosition, key,x)
                remainingValues.[key] <- (values |> List.tail)
                let remaining = (eventPosition + 1, t, remainingValues)
                Some (nextValue, remaining)

        (0, streamValueOrdering, dictionary) 
        |> Seq.unfold generateStream
        
    let sequentialNumbers streamCount valuesPerStreamCount = 

        sequentialValues streamCount valuesPerStreamCount
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