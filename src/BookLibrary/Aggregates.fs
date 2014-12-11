namespace BookLibrary

open System

open FSharpx
open Eventful
open Eventful.AggregateActionBuilder

[<AttributeUsage(AttributeTargets.Property)>]
type GeneratedIdAttribute () =
    class
        inherit System.Attribute()
    end

[<AttributeUsage(AttributeTargets.Property)>]
type FromRouteAttribute () =
    class
        inherit System.Attribute()
    end

type IEvent = interface end

type AggregateType =
    | Book = 1
    | BookCopy = 2
    | Award = 3
    | Delivery = 4

type BookLibraryEventMetadata = {
    SourceMessageId: string
    EventTime : DateTime
    AggregateType : AggregateType
}
with 
    static member GetUniqueId x = Some x.SourceMessageId
    static member GetAggregateType x = x.AggregateType

type BookLibraryEventContext = {
    Metadata : BookLibraryEventMetadata
    EventId : Guid
}
with 
    interface IDisposable with
        member x.Dispose() = ()

module Aggregates = 

    let stateBuilder<'TId when 'TId : equality> = StateBuilder.nullStateBuilder<BookLibraryEventMetadata, 'TId>

    let emptyMetadata aggregateType sourceMessageId = { 
        SourceMessageId = sourceMessageId 
        EventTime = DateTime.UtcNow 
        AggregateType = aggregateType
    }

    let cmdHandlerS stateBuilder f buildMetadata =
        AggregateActionBuilder.fullHandler
            stateBuilder
            (fun state () cmd -> 
                let events = 
                    f state cmd 
                    |> (fun evt -> (evt :> IEvent, buildMetadata))
                    |> Seq.singleton

                let uniqueId = Guid.NewGuid().ToString()

                {
                    UniqueId = uniqueId
                    Events = events
                }
                |> Choice1Of2
            )
        |> AggregateActionBuilder.buildCmd

    let cmdHandler f =
        cmdHandlerS StateBuilder.nullStateBuilder (fun _ -> f)

    let inline linkEvent fId buildMetadata =
        Eventful.AggregateActionBuilder.linkEvent fId buildMetadata

    let inline onEvent fId sb f =
        let handler state event (context : BookLibraryEventContext) =
            {
                UniqueId = context.EventId.ToString()
                Events = f state event
            }
            
        Eventful.AggregateActionBuilder.onEvent fId sb handler

    let toAggregateDefinition = Eventful.Aggregate.toAggregateDefinition