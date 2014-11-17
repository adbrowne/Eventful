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

    let metadataBuilder messageId sourceMessageId = { 
        MessageId = messageId 
        SourceMessageId = sourceMessageId 
        AggregateType =  "TestAggregate" }

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

    let cmdHandler f =
        AggregateActionBuilder.fullHandler
            StateBuilder.nullStateBuilder
            (fun _ (cmdContext : Guid) cmd -> 
                let events = 
                    f cmd 
                    |> (fun evt -> (evt :> IEvent, metadataBuilder))
                    |> Seq.singleton

                let uniqueId = cmdContext.ToString()

                {
                    UniqueId = uniqueId
                    Events = events
                }
                |> Choice1Of2
            )