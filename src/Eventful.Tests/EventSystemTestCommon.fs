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

    let metadataBuilder sourceMessageId = { 
        SourceMessageId = sourceMessageId 
        AggregateType =  "TestAggregate" }

    let getCommandStreamName _ (id : Guid) = 
        sprintf "Foo-%s" <| id.ToString("N")

    let getStreamName UnitEventContext (id : Guid) =
        sprintf "Foo-%s" <| id.ToString("N")

    let barEventCounter : IStateBuilder<int, TestMetadata, Guid> =
        StateBuilder.eventTypeCountBuilder (fun (e:BarEvent) _ -> e.Id)
        |> StateBuilder.toInterface

    let multiEventCmdHandler f =
        AggregateActionBuilder.fullHandler
            StateBuilder.nullStateBuilder
            (fun _ (cmdContext : Guid) cmd -> 
                let events = 
                    f cmd 
                    |> Seq.map (fun evt -> (evt :> IEvent, metadataBuilder))

                let uniqueId = cmdContext.ToString()

                {
                    UniqueId = uniqueId
                    Events = events
                }
                |> Choice1Of2
            )

    let cmdHandler f =
        multiEventCmdHandler (f >> Seq.singleton)