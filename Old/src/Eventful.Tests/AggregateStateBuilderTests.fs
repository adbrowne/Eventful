﻿namespace Eventful.Tests

open System

open Xunit
open FsUnit.Xunit

open Eventful

module AggregateStateBuilderTests = 
    type SampleMetadata = {
        Tenancy : string
        AggregateId : Guid 
    }
    with static member Emtpy = { Tenancy = ""; AggregateId = Guid.Empty }

    type WidgetCreatedEvent = {
        WidgetId : Guid
        Name : string
    }

    type WidgetRenamedEvent = {
        WidgetId : Guid
        NewName : string
    }

    type WidgetNameAppendedToEvent = {
        WidgetId : Guid
        Suffix : string
    }

    let widgetNameStateBuilder : StateBuilder<string,SampleMetadata,Guid> = 
        StateBuilder.Empty "WidgetName" ""
        |> StateBuilder.handler (fun (e:WidgetCreatedEvent) m -> e.WidgetId) (fun (s,e,m) -> e.Name)
        |> StateBuilder.handler (fun (e:WidgetRenamedEvent) m -> e.WidgetId) (fun (s,e,m) -> e.NewName)
        |> StateBuilder.handler (fun (e:WidgetNameAppendedToEvent) m -> e.WidgetId) (fun (s,e,m) -> sprintf "%s%s" s e.Suffix)

    let widgetEventCountBuilder : StateBuilder<int, SampleMetadata, Guid> =
        StateBuilder.Empty "EventCount" 0
        |> StateBuilder.allEventsHandler (fun m -> m.AggregateId) (fun (s,e,m) -> s + 1)

    let widgetId = Guid.Parse("2F8A016D-FB2C-4857-8E2F-5E81FB4F95DA")
    let metadata = { Tenancy = "Blue"; AggregateId = widgetId }

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can calculate simple state`` () =
        (widgetNameStateBuilder, widgetNameStateBuilder.InitialState)
        |> StateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> snd
        |> should equal "My Widget Name"

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can run multiple events`` () =
        (widgetNameStateBuilder, widgetNameStateBuilder.InitialState)
        |> StateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> StateBuilder.run widgetId { WidgetId = widgetId; NewName = "My NEW Widget Name"} metadata
        |> snd
        |> should equal "My NEW Widget Name"

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can use previous state`` () =
        (widgetNameStateBuilder, widgetNameStateBuilder.InitialState)
        |> StateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> StateBuilder.run widgetId { WidgetId = widgetId; Suffix = "My Suffix"} metadata
        |> snd
        |> should equal "My Widget NameMy Suffix"

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can Get Keys From Event`` () =
        let evt = { WidgetId = widgetId; Name = "My Widget Name"}
        widgetNameStateBuilder
        |> StateBuilder.getKeys evt metadata
        |> Seq.toList
        |> should equal [widgetId]

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can count events`` () =
        (widgetEventCountBuilder, widgetEventCountBuilder.InitialState)
        |> StateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> StateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> snd
        |> should equal 2

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can combine state`` () =
        let tupledState = AggregateStateBuilder.tuple2 widgetNameStateBuilder widgetEventCountBuilder

        Map.empty
        |> AggregateStateBuilder.run tupledState.GetBlockBuilders widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> AggregateStateBuilder.run tupledState.GetBlockBuilders widgetId { WidgetId = widgetId; NewName = "My NEW Widget Name"} metadata
        |> tupledState.GetState
        |> should equal ("My NEW Widget Name", 2)