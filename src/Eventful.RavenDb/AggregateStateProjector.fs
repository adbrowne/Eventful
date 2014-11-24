namespace Eventful.Raven

open System
open FSharpx

open Raven.Json.Linq
open Eventful

module AggregateStateProjector =
    let deserialize (serializer :  ISerializer) (doc : RavenJObject) (blockBuilders : IStateBlockBuilder<'TMetadata, unit> list) =
        let deserializeRavenJToken targetType jToken =
            jToken.ToString()
            |> System.Text.Encoding.UTF8.GetBytes
            |> (fun x -> serializer.DeserializeObj x targetType)

        let blockBuilderMap = 
            blockBuilders
            |> Seq.map(fun b -> b.Name, b)
            |> Map.ofSeq

        let addKey stateMap key =
            let blockBuilder = blockBuilderMap.Item key
            let blockType = blockBuilder.Type
            let jToken = doc.Item key
            let value = deserializeRavenJToken blockType jToken
            stateMap |> Map.add key value

        doc.Keys
        |> Seq.fold addKey Map.empty

    let getDocumentKey streamId = 
        "Snapshot/" + streamId

    let mapToRavenJObject (serializer : ISerializer) (stateMap : Map<string,obj>) =
        let jObject = new RavenJObject()
        for keyValuePair in stateMap do
            jObject.Add(keyValuePair.Key, RavenJToken.Parse(System.Text.Encoding.UTF8.GetString <| serializer.Serialize keyValuePair.Value))
        jObject

    let buildProjector 
        (getStreamId : 'TMessage -> string) 
        (getEventNumber : 'TMessage -> int)
        (getEvent : 'TMessage -> obj) 
        (getMetadata : 'TMessage -> 'TMetadata) 
        (serializer :  ISerializer)
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent,'TAggregateType>) =
        let matchingKeys = 
            getStreamId >> Seq.singleton

        let processEvents 
            (fetcher : IDocumentFetcher) 
            (streamId : string) 
            (messages : seq<'TMessage>) = async {
                
                let documentKey = getDocumentKey streamId
                let! doc = 
                    fetcher.GetDocument documentKey
                    |> Async.AwaitTask

                // assumption we will never get empty message list
                let aggregateType = 
                    messages 
                    |> Seq.map getMetadata
                    |> Seq.map handlers.GetAggregateType
                    |> Seq.distinct
                    |> Seq.toList
                    |> function
                        | [aggregateType] -> aggregateType
                        | x -> failwith <| sprintf "Got messages for mixed aggreate type. Stream: %s, AggregateTypes %A" streamId x

                // todo check event has not been applied

                return
                    match handlers.AggregateTypes |> Map.tryFind aggregateType with
                    | Some aggregateConfig ->
                        let (snapshot : RavenJObject, metadata : RavenJObject, etag) = 
                            doc
                            |> Option.getOrElseF (fun () -> (new RavenJObject(), fetcher.GetEmptyMetadata(), Raven.Abstractions.Data.Etag.Empty))

                        let snapshot = 
                            deserialize serializer snapshot aggregateConfig.StateBuilder.GetBlockBuilders

                        let applyToSnapshot snapshot message =
                            let event = getEvent message
                            let metadata = getMetadata message
                            snapshot
                            |> AggregateStateBuilder.dynamicRun aggregateConfig.StateBuilder.GetBlockBuilders () event metadata 

                        let updatedSnapshot =
                            messages
                            |> Seq.fold applyToSnapshot snapshot
                            |> mapToRavenJObject serializer
                            // todo write max event number to metadata

                        let writeDoc = 
                            ProcessAction.Write (
                                {
                                    DocumentKey = documentKey
                                    Document = updatedSnapshot
                                    Metadata = lazy(metadata)
                                    Etag = etag
                                } , Guid.NewGuid())

                        Seq.singleton writeDoc
                    | None ->
                        failwith <| sprintf "Could not find configuration for aggregateType: %A" aggregateType
        }

        {
            MatchingKeys = matchingKeys
            ProcessEvents = processEvents
        }