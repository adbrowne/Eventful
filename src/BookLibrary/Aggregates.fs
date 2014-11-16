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

type BookLibraryEventMetadata = {
    MessageId: Guid
    AggregateId : Guid
    SourceMessageId: string
    EventTime : DateTime
}
with 
    static member GetUniqueId x = Some x.SourceMessageId

module Aggregates = 

    let stateBuilder<'TId when 'TId : equality> = StateBuilder.nullStateBuilder<BookLibraryEventMetadata, 'TId>

    let emptyMetadata aggregateId messageId sourceMessageId = { SourceMessageId = sourceMessageId; MessageId = messageId; EventTime = DateTime.UtcNow; AggregateId = aggregateId }

    let cmdHandlerS stateBuilder f buildMetadata =
        AggregateActionBuilder.fullHandler
            stateBuilder
            (fun state () cmd -> 
                let events = 
                    f state cmd 
                    |> (fun evt -> (evt :> obj, buildMetadata))
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
        Eventful.AggregateActionBuilder.onEvent fId sb f

    let toAggregateDefinition = Eventful.Aggregate.toAggregateDefinition