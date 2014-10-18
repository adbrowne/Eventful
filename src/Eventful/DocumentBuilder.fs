namespace Eventful

type IDocumentStateMap<'TDocument, 'TMetadata, 'TKey> =
    abstract member GetKey<'TEvent> : 'TEvent * 'TMetadata -> 'TKey seq
    abstract member Apply<'TEvent> : string * 'TDocument * 'TEvent * 'TMetadata -> 'TDocument

type DocumentBuilder<'TKey,'T, 'TMetadata when 'TKey : equality>(createDoc:'TKey -> 'T, getDocumentKey:'TKey -> string, stateMaps: IDocumentStateMap<'T, 'TMetadata, 'TKey> list) =
    static member Empty<'TKey,'T> createDoc getDocumentKey = new DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc, getDocumentKey, [])
    member x.AddStateMap stateMap =
        new DocumentBuilder<'TKey,'T, 'TMetadata>(createDoc, getDocumentKey, stateMap::stateMaps)
    member x.GetDocumentKey = getDocumentKey
    member x.GetKeysFromEvent (evt:'TEvent, metadata : 'TMetadata) : string list =
        stateMaps 
        |> List.map (fun x -> x.GetKey (evt, metadata))
        |> Seq.collect id
        |> Seq.distinct
        |> Seq.map getDocumentKey
        |> List.ofSeq
    member x.NewDocument key = createDoc key
    member x.ApplyEvent (key : string, currentDocument : 'T, evt:'TEvent, metadata : 'TMetadata) : 'T = 
        let applyStateMap doc (stateMap : IDocumentStateMap<'T, 'TMetadata, 'TKey>) =
            stateMap.Apply (key, doc, evt, metadata)
        List.fold applyStateMap currentDocument stateMaps

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DocumentBuilder =
    let mapStateToProperty (sb:StateBuilder<'TProperty, 'TMetadata,'TKey>) (getter:'T -> 'TProperty) (setter:'TProperty -> 'T -> 'T) (builder : DocumentBuilder<'TKey,'T, 'TMetadata>)  =
       let stateMap = 
           {
               new IDocumentStateMap<'T, 'TMetadata, 'TKey> with 
                   member this.Apply (key, document, evt, metadata) = 
                        let currentValue = getter document
                        let handlers = 
                            sb.GetRunners<'TEvent>()
                            |> Seq.map (fun (getKey,runner) -> (getKey evt metadata, runner))
                            |> Seq.filter(fun (k, _) -> builder.GetDocumentKey k = key)
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