namespace Eventful.Tests

open System

open Xunit
open FsUnit.Xunit

open Eventful

module DocumentBuilderTests =
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

    let inline getWidgetId (a: ^a) _ = 
        (^a : (member WidgetId: Guid) (a))

    let widgetNameStateBuilder : UnitStateBuilder<string,SampleMetadata,Guid> = 
        UnitStateBuilder.Empty "WidgetName" ""
        |> UnitStateBuilder.handler getWidgetId (fun (s, (e:WidgetCreatedEvent), m) -> e.Name)
        |> UnitStateBuilder.handler getWidgetId (fun (s, (e:WidgetRenamedEvent), m) -> e.NewName)

    type WidgetDocument = {
        WidgetId : Guid
        Name : string } 
    with static member Empty id = { WidgetId = id; Name = "" }

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can map state builders to documents`` () =
        let getDocumentKey docId = sprintf "WidgetDocument/%s" (docId.ToString())
        let visitDocumentBuilder = 
            DocumentBuilder.Empty<Guid, WidgetDocument> (fun x -> WidgetDocument.Empty x) getDocumentKey
            |> DocumentBuilder.mapStateToProperty widgetNameStateBuilder (fun doc -> doc.Name) (fun name doc -> { doc with Name = name })

        // visitDocumentBuilder.EventTypes |> List.exists (fun x -> x = typeof<WidgetRenamedEvent>) |> should equal true
        let guid = Guid.Parse("75ca0d81-7a8e-4692-86ac-7f128deb75bd")
        visitDocumentBuilder.GetDocumentKey guid |> should equal "WidgetDocument/75ca0d81-7a8e-4692-86ac-7f128deb75bd"

        let newNameEvent = { WidgetId = guid; NewName = "New Name"}
        let metadata = { Tenancy = ""; AggregateId = guid }
        visitDocumentBuilder.GetKeysFromEvent (newNameEvent, metadata)
        |> should equal ["WidgetDocument/75ca0d81-7a8e-4692-86ac-7f128deb75bd"]

        let emptyDocument = visitDocumentBuilder.NewDocument guid
        emptyDocument |> should equal { WidgetDocument.WidgetId = guid; Name = ""}

        visitDocumentBuilder.ApplyEvent (getDocumentKey guid,emptyDocument,newNameEvent, metadata) 
        |> (fun x -> x.Name)
        |> should equal "New Name"