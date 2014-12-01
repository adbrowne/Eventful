﻿namespace Eventful.Raven

open System
open FSharpx
open FSharpx.Option
open Raven.Json.Linq
open Eventful

type AggregateState = {
    Snapshot : StateSnapshot
    NextWakeup : DateTime option
}

module AggregateStatePersistence =
    type AggregateStateDocument<'TAggregateType> = {
        Snapshot : RavenJObject
        LastEventNumber : int
        NextWakeup : string
        AggregateType : 'TAggregateType
    }

    let stateDocumentCollectionName = "AggregateStates"

    let wakeupIndexName =  "EventfulWakeup"
    let wakeupTimeFieldName = "WakeupTime"
    let wakeupIndex () = 
        let definition = new Raven.Abstractions.Indexing.IndexDefinition()

        definition.Name <- wakeupIndexName
        definition.Map <- sprintf "docs.%s.Where(state => state.NextWakeup != null).Select(state => new { %s = state.NextWakeup, AggregateType = state.AggregateType })" stateDocumentCollectionName wakeupTimeFieldName
        definition.SortOptions.Add("WakeupTime", Raven.Abstractions.Indexing.SortOptions.String)
        definition.Stores.Add("WakeupTime", Raven.Abstractions.Indexing.FieldStorage.Yes)
        definition.Stores.Add("AggregateType", Raven.Abstractions.Indexing.FieldStorage.Yes)
        definition

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

    let documentKeyPrefix = "AggregateState/"
    let getDocumentKey streamId = 
        documentKeyPrefix + streamId

    let emptyMetadata () =
        let metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase)
        metadata.Add("Raven-Entity-Name", new RavenJValue(stateDocumentCollectionName))
        metadata

    let mapToRavenJObject (serializer : ISerializer) (stateMap : Map<string,obj>) =
        let jObject = new RavenJObject()
        for keyValuePair in stateMap do
            jObject.Add(keyValuePair.Key, RavenJToken.Parse(System.Text.Encoding.UTF8.GetString <| serializer.Serialize keyValuePair.Value))
        jObject

    let deserializeDateString (value : string) =
        match value with
        | null -> None
        | value -> 
            Some (DateTime.Parse value)

    let serializeDateTimeOption = function
        | None -> null
        | Some (dateTime : DateTime) -> dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

    let getAggregateState   
        (documentStore : Raven.Client.Document.DocumentStore) 
        serializer 
        (database : string) 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent,'TAggregateType>)
        streamId 
        aggregateType
        = 
        async {
        let stateDocumentKey = getDocumentKey streamId
        use session = documentStore.OpenAsyncSession(database)
        let! doc = session.LoadAsync<AggregateStateDocument<'TAggregateType>> stateDocumentKey |> Async.AwaitTask

        let blockBuilders = (handlers.AggregateTypes.Item aggregateType).StateBuilder.GetBlockBuilders
        let snapshot = deserialize serializer doc.Snapshot blockBuilders
        return {
            AggregateState.Snapshot = { StateSnapshot.State =  snapshot; LastEventNumber = doc.LastEventNumber }
            NextWakeup = deserializeDateString doc.NextWakeup
        }
    }

    let applyMessages (streamConfig : EventfulStreamConfig<_>) (stateSnapshot : StateSnapshot) persistedEvents =
        let runEvent = 
            AggregateStateBuilder.dynamicRun streamConfig.StateBuilder.GetBlockBuilders ()
            
        let applyToSnapshot (stateSnapshot : StateSnapshot) (persistedEvent : PersistedEvent<_>) =
             if persistedEvent.EventNumber > stateSnapshot.LastEventNumber then
                { 
                    StateSnapshot.LastEventNumber = persistedEvent.EventNumber; 
                    State = 
                        stateSnapshot.State
                        |> runEvent persistedEvent.Body persistedEvent.Metadata
                }
             else stateSnapshot

        persistedEvents
        |> Seq.fold applyToSnapshot stateSnapshot

    let optionToSeq = function
    | Some x -> Seq.singleton x
    | None -> Seq.empty

    let buildProjector 
        (getPersistedEvent : 'TMessage -> PersistedEvent<'TMetadata> option)
        (serializer :  ISerializer)
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent,'TAggregateType>) =

        let matchingKeys = 
            getPersistedEvent
            >> Option.map (fun x -> x.StreamId)
            >> optionToSeq

        let getPersistedEvents =
            Seq.map getPersistedEvent
            >> Seq.collect optionToSeq

        let getAggregateConfig streamId persistedEvents =
            let aggregateType = 
                persistedEvents
                |> Seq.map (fun (x : PersistedEvent<_>) -> x.Metadata)
                |> Seq.map handlers.GetAggregateType
                |> Seq.distinct
                |> Seq.toList
                |> function
                    | [aggregateType] -> aggregateType
                    | x -> failwith <| sprintf "Got messages for mixed aggreate type. Stream: %s, AggregateTypes: %A" streamId x

            match handlers.AggregateTypes |> Map.tryFind aggregateType with
            | Some aggregateConfig -> 
                (persistedEvents, aggregateType, aggregateConfig)
            | None -> 
                failwith <| sprintf "Could not find configuration for aggregateType: %A" aggregateType

        let loadCurrentState (fetcher : IDocumentFetcher) streamId (persistedEvents, aggregateType, (aggregateConfig : EventfulStreamConfig<_>)) = async {
            let documentKey = getDocumentKey streamId

            let! doc = 
                fetcher.GetDocument documentKey
                |> Async.AwaitTask

            let (snapshot : StateSnapshot, metadata : RavenJObject, etag) = 
                doc
                |> Option.map (fun (stateDocument : AggregateStateDocument<'TAggregateType>, metadata : RavenJObject, etag) ->
                    ({ 
                        StateSnapshot.LastEventNumber = stateDocument.LastEventNumber
                        State = deserialize serializer stateDocument.Snapshot aggregateConfig.StateBuilder.GetBlockBuilders
                    }, metadata, etag)
                )
                |> Option.getOrElseF (fun () -> 
                    (StateSnapshot.Empty, emptyMetadata(), Raven.Abstractions.Data.Etag.Empty))

            let docMetadata = (documentKey, metadata, etag)
            return (persistedEvents, aggregateType, aggregateConfig, snapshot, docMetadata)
        }

        let applyEventsToSnapshot (persistedEvents, aggregateType, aggregateConfig, snapshot, docMetadata) =
            let snapshot' = applyMessages aggregateConfig snapshot persistedEvents
            (persistedEvents, aggregateType, aggregateConfig, snapshot', docMetadata)

        let computeNextWakeup (persistedEvents, aggregateType, (aggregateConfig : EventfulStreamConfig<_>), snapshot, docMetadata) =
            let nextWakeup = 
                maybe {
                    let! EventfulWakeupHandler (nextWakeupStateBuilder,_) = aggregateConfig.Wakeup
                    return! nextWakeupStateBuilder.GetState snapshot.State
                }
            (persistedEvents, aggregateType, aggregateConfig, snapshot, docMetadata, nextWakeup)

        let createWriteRequest (_, aggregateType, (aggregateConfig : EventfulStreamConfig<_>), (snapshot : StateSnapshot), (documentKey, metadata, etag), nextWakeup) =
            let updatedDoc = {
                AggregateStateDocument.Snapshot = mapToRavenJObject serializer snapshot.State
                LastEventNumber = snapshot.LastEventNumber
                NextWakeup = serializeDateTimeOption nextWakeup
                AggregateType = aggregateType
            }

            ProcessAction.Write (
                {
                    DocumentKey = documentKey
                    Document = updatedDoc
                    Metadata = lazy(metadata)
                    Etag = etag
                } , Guid.NewGuid())

        let processEvents 
            (fetcher : IDocumentFetcher) 
            (streamId : string) 
            =
                getPersistedEvents
                >> (getAggregateConfig streamId)
                >> (loadCurrentState fetcher streamId)
                >> Async.map (
                    applyEventsToSnapshot
                    >> computeNextWakeup
                    >> createWriteRequest
                    >> Seq.singleton
                )
        {
            MatchingKeys = matchingKeys
            ProcessEvents = processEvents
        }