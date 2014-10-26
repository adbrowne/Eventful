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

type AggregateType =
| Book
| BookCopy

type BookLibraryEventMetadata = {
    MessageId: Guid
    AggregateId : Guid
    SourceMessageId: string
    EventTime : DateTime
}

module Aggregates = 
    let systemConfiguration getId = {
        SystemConfiguration.GetUniqueId = (fun x -> Some x.SourceMessageId)
        GetAggregateId = getId
    }

    let stateBuilder<'TId when 'TId : equality> = StateBuilder.nullStateBuilder<BookLibraryEventMetadata, 'TId>

    let emptyMetadata aggregateId messageId sourceMessageId = { SourceMessageId = sourceMessageId; MessageId = messageId; EventTime = DateTime.UtcNow; AggregateId = aggregateId }

    let cmdHandlerS getId stateBuilder f =
        AggregateActionBuilder.fullHandler
            (systemConfiguration  getId)
            stateBuilder
            (fun state () cmd -> 
                let events = 
                    f state cmd 
                    |> (fun evt -> (evt :> obj, emptyMetadata))
                    |> Seq.singleton

                let uniqueId = Guid.NewGuid().ToString()

                {
                    UniqueId = uniqueId
                    Events = events
                }
                |> Choice1Of2
            )
        |> AggregateActionBuilder.buildCmd

    let cmdHandler getId f =
        cmdHandlerS getId StateBuilder.nullStateBuilder (fun _ -> f)

    let inline linkEvent fId =
        Eventful.AggregateActionBuilder.linkEvent fId emptyMetadata

    let inline onEvent fId sb f =
        Eventful.AggregateActionBuilder.onEvent fId sb f

    let toAggregateDefinition = Eventful.Aggregate.toAggregateDefinition