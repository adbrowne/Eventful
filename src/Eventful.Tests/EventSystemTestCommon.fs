namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing

open Xunit
open FsUnit.Xunit

module EventSystemTestCommon = 
    type BarEvent = {
        Id : Guid
    }

    let metadataBuilder aggregateId messageId sourceMessageId = { 
        TestMetadata.AggregateId = aggregateId
        MessageId = messageId 
        SourceMessageId = sourceMessageId }

    let addEventType evtType handlers =
        handlers
        |> EventfulHandlers.addClassToEventStoreType evtType evtType.Name
        |> EventfulHandlers.addEventStoreType evtType.Name evtType 

    let addEventTypes evtTypes handlers =
        Seq.fold (fun h x -> addEventType x h) handlers evtTypes

    let getCommandStreamName _ (id : Guid) = 
        sprintf "Foo-%s" <| id.ToString("N")

    let getStreamName () (id : Guid) =
        sprintf "Foo-%s" <| id.ToString("N")

    let barEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:BarEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    let systemConfiguration = {
        SystemConfiguration.GetUniqueId = (fun (x : TestMetadata) -> Some x.SourceMessageId)
        GetAggregateId = (fun (x : TestMetadata) -> x.AggregateId)
    }

    let cmdHandler f =
        AggregateActionBuilder.fullHandler
            systemConfiguration 
            StateBuilder.nullStateBuilder
            (fun _ (cmdContext : Guid) cmd -> 
                let events = 
                    f cmd 
                    |> (fun evt -> (evt :> obj, metadataBuilder))
                    |> Seq.singleton

                let uniqueId = cmdContext.ToString()

                {
                    UniqueId = uniqueId
                    Events = events
                }
                |> Choice1Of2
            )
