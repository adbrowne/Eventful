namespace Eventful.Tests

open System

open Xunit
open FsUnit.Xunit

open Eventful

module DocumentBuilderTests =
    type SampleMetadata = {
        Tenancy : string
    }

    type WidgetCreatedEvent = {
        WidgetId : Guid
        Name : string
    }

    type WidgetRenamedEvent = {
        WidgetId : Guid
        NewName : string
    }

    let widgetNameStateBuilder : NamedStateBuilder<string,SampleMetadata> = 
        StateBuilder.Empty ""
        |> StateBuilder.addHandler (fun s (e:WidgetCreatedEvent) -> e.Name)
        |> StateBuilder.addHandler (fun s (e:WidgetRenamedEvent) -> e.NewName)
        |> NamedStateBuilder.withName "WidgetName"

    type WidgetDocument = {
        WidgetId : Guid
        Name : string
    }

    type IDocumentStateMap<'TDocument, 'TMetadata> =
        abstract member Types : List<Type>
        abstract member Apply : (obj * 'TMetadata) -> 'TDocument -> 'TDocument

    type DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc:'TKey -> 'T, stateMaps: IDocumentStateMap<'T, 'TMetadata> list) =
        static member Empty<'TKey,'T> createDoc = new DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc, [])
        member x.AddStateMap stateMap =
            new DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc, stateMap::stateMaps)
        member x.EventTypes =
            stateMaps |> List.collect (fun x -> x.Types)

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module DocumentBuilder =
        let mapStateToProperty (sb:NamedStateBuilder<'TProperty, SampleMetadata>) (getter:'T -> 'TProperty) (setter:'TProperty -> 'T -> 'T) (builder : DocumentBuilder<'TKey,'T, 'TMetadata>)  =
           let stateBuilderInterface = sb :> INamedStateBuilder<SampleMetadata>
           let stateMap = 
               {
                   new IDocumentStateMap<'T, 'TMetadata> with 
                       member this.Types = sb.MessageTypes
                       member this.Apply msg document = 
                            let currentValue = getter document
                            let updated = sb.Apply currentValue msg
                            setter updated document
               } 
           builder.AddStateMap(stateMap)

    [<Fact>]
    let ``Can map state builders to documents`` () =
        let visitDocumentBuilder = 
            DocumentBuilder.Empty<Guid, WidgetDocument> (fun x -> { WidgetDocument.WidgetId = x; Name = "" })
            |> DocumentBuilder.mapStateToProperty widgetNameStateBuilder (fun doc -> doc.Name) (fun name doc -> { doc with Name = name })

        visitDocumentBuilder.EventTypes |> List.exists (fun x -> x = typeof<WidgetRenamedEvent>) |> should equal true