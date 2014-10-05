namespace Eventful.Tests

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

    let widgetNameStateBuilder : UnitStateBuilder<string,SampleMetadata,Guid> = 
        UnitStateBuilder.Empty "WidgetName" ""
        |> UnitStateBuilder.handler (fun (e:WidgetCreatedEvent) m -> e.WidgetId) (fun (s,e,m) -> e.Name)
        |> UnitStateBuilder.handler (fun (e:WidgetRenamedEvent) m -> e.WidgetId) (fun (s,e,m) -> e.NewName)
        |> UnitStateBuilder.handler (fun (e:WidgetNameAppendedToEvent) m -> e.WidgetId) (fun (s,e,m) -> sprintf "%s%s" s e.Suffix)

    let widgetEventCountBuilder : UnitStateBuilder<int, SampleMetadata, Guid> =
        UnitStateBuilder.Empty "EventCount" 0
        |> UnitStateBuilder.allEventsHandler (fun m -> m.AggregateId) (fun (s,e,m) -> s + 1)

    let metadata = { Tenancy = "Blue"; AggregateId = Guid.NewGuid() }
    let widgetId = Guid.Parse("2F8A016D-FB2C-4857-8E2F-5E81FB4F95DA")

    [<Fact>]
    let ``Can calculate simple state`` () =
        (widgetNameStateBuilder, widgetNameStateBuilder.InitialState)
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> snd
        |> should equal "My Widget Name"

    [<Fact>]
    let ``Can run multiple events`` () =
        (widgetNameStateBuilder, widgetNameStateBuilder.InitialState)
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; NewName = "My NEW Widget Name"} metadata
        |> snd
        |> should equal "My NEW Widget Name"

    [<Fact>]
    let ``Can use previous state`` () =
        (widgetNameStateBuilder, widgetNameStateBuilder.InitialState)
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; Suffix = "My Suffix"} metadata
        |> snd
        |> should equal "My Widget NameMy Suffix"

    [<Fact>]
    let ``Can Get Keys From Event`` () =
        let evt = { WidgetId = widgetId; Name = "My Widget Name"}
        widgetNameStateBuilder
        |> UnitStateBuilder.getKeys evt metadata
        |> Seq.toList
        |> should equal [widgetId]

    [<Fact>]
    let ``Can count events`` () =
        (widgetEventCountBuilder, widgetEventCountBuilder.InitialState)
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> UnitStateBuilder.run widgetId { WidgetId = widgetId; Name = "My Widget Name"} metadata
        |> snd
        |> should equal 2