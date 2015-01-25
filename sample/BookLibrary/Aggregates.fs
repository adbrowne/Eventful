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
    | NewArrivalsNotification = 5

type BookLibraryEventMetadata = {
    SourceMessageId: string option
    EventTime : DateTime
    AggregateType : AggregateType
}
with 
    static member GetUniqueId x = x.SourceMessageId
    static member GetAggregateType x = x.AggregateType.ToString()

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
             MagicMapper.magicGetCmdId<'TId>
            stateBuilder
            (fun state () cmd -> 
                f state cmd 
                |> (fun evt -> (evt :> IEvent, buildMetadata None))
                |> Seq.singleton
                |> Choice1Of2
            )
        |> AggregateActionBuilder.buildCmd

    let cmdHandler f =
        cmdHandlerS StateBuilder.nullStateBuilder (fun _ -> f)

    let inline linkEvent fId buildMetadata =
        Eventful.AggregateActionBuilder.linkEvent fId buildMetadata

    let inline onEvent fId sb f =
        let handler state event (context : BookLibraryEventContext) =
            f state event
            |> Seq.map (fun (evt, buildMetadata) -> (evt, buildMetadata (Some (context.EventId.ToString()))))
            
        Eventful.AggregateActionBuilder.onEvent fId sb handler

    let toAggregateDefinition 
        (aggregateType : AggregateType)
        getCommandStreamName
        getEventStreamName
        cmdHandlers
        evtHandlers
        = 
        Eventful.Aggregate.toAggregateDefinition
            (aggregateType.ToString())
            BookLibraryEventMetadata.GetUniqueId
            getCommandStreamName
            getEventStreamName
            cmdHandlers
            evtHandlers