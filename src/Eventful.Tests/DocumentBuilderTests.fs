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

    type IDocumentStateMap<'TDocument, 'TMetadata, 'TKey> =
        abstract member GetKey<'TEvent> : 'TEvent * 'TMetadata -> 'TKey seq
        abstract member Apply<'TEvent> : 'TKey * 'TDocument * 'TEvent * 'TMetadata -> 'TDocument

    type DocumentBuilder<'TKey,'T, 'TMetadata when 'TKey : equality>(createDoc:'TKey -> 'T, getDocumentKey:'TKey -> string, stateMaps: IDocumentStateMap<'T, 'TMetadata, 'TKey> list) =
        static member Empty<'TKey,'T> createDoc getDocumentKey = new DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc, getDocumentKey, [])
        member x.AddStateMap stateMap =
            new DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc, getDocumentKey, stateMap::stateMaps)
        member x.GetDocumentKey = getDocumentKey
        member x.GetKeysFromEvent (evt:'TEvent, metadata : 'TMetadata) : 'TKey list =
            stateMaps 
            |> List.map (fun x -> x.GetKey (evt, metadata))
            |> Seq.collect id
            |> Seq.distinct
            |> List.ofSeq

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module DocumentBuilder =
        let mapStateToProperty (sb:UnitStateBuilder<'TProperty, 'TMetadata,'TKey>) (getter:'T -> 'TProperty) (setter:'TProperty -> 'T -> 'T) (builder : DocumentBuilder<'TKey,'T, 'TMetadata>)  =
           let stateMap = 
               {
                   new IDocumentStateMap<'T, 'TMetadata, 'TKey> with 
                       member this.Apply (key, document, evt, metadata) = 
                            let currentValue = getter document
                            let handlers = 
                                sb.GetRunners<'TEvent>()
                                |> Seq.map (fun (getKey,runner) -> (getKey evt metadata, runner))
                                |> Seq.filter(fun (k, _) -> k = key)
                                |> Seq.map snd

                            let runHandler v h = h evt metadata v

                            let updated = handlers |> Seq.fold runHandler currentValue
                            setter updated document
                       member this.GetKey<'TEvent>(evt : 'TEvent, metadata) = 
                            sb.GetRunners<'TEvent>()
                            |> Seq.map fst
                            |> Seq.map (fun f -> f evt metadata)
               }
           builder.AddStateMap(stateMap)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can map state builders to documents`` () =
        let visitDocumentBuilder = 
            DocumentBuilder.Empty<Guid, WidgetDocument> (fun x -> WidgetDocument.Empty x) (fun x -> sprintf "WidgetDocument/%s" (x.ToString()))
            |> DocumentBuilder.mapStateToProperty widgetNameStateBuilder (fun doc -> doc.Name) (fun name doc -> { doc with Name = name })

        // visitDocumentBuilder.EventTypes |> List.exists (fun x -> x = typeof<WidgetRenamedEvent>) |> should equal true
        let guid = Guid.Parse("75ca0d81-7a8e-4692-86ac-7f128deb75bd")
        visitDocumentBuilder.GetDocumentKey guid |> should equal "WidgetDocument/75ca0d81-7a8e-4692-86ac-7f128deb75bd"

        visitDocumentBuilder.GetKeysFromEvent ({ WidgetId = guid; NewName = "New Name"}, { Tenancy = ""; AggregateId = guid })
        |> should equal [guid]