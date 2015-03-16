namespace Eventful.Raven

open System
open FSharpx
open FSharpx.Option
open Raven.Json.Linq
open Eventful

type AggregateState = {
    Snapshot : StateSnapshot
    NextWakeup : UtcDateTime option
}

module AggregateStatePersistence =
    type AggregateStateDocument = {
        Snapshot : RavenJObject
        LastEventNumber : int
        NextWakeup : string
        // stored explicitly instead of computing from document key
        // as it is case sensitive
        StreamId : string 
        AggregateType : string
    }

    let stateDocumentCollectionName = "AggregateStates"

    let wakeupIndexName =  "EventfulWakeup"
    let wakeupTimeFieldName = "WakeupTime"
    let streamIdFieldName = "StreamId"
    let aggregateTypeFieldName = "AggregateType"
    let wakeupIndex () = 
        let definition = new Raven.Abstractions.Indexing.IndexDefinition()

        definition.Name <- wakeupIndexName
        definition.Map <- sprintf "docs.%s.Where(state => state.NextWakeup != null).Select(state => new { %s = state.NextWakeup, AggregateType = state.AggregateType, StreamId = state.StreamId })" stateDocumentCollectionName wakeupTimeFieldName
        definition.SortOptions.Add("WakeupTime", Raven.Abstractions.Indexing.SortOptions.String)
        definition.Stores.Add(wakeupTimeFieldName, Raven.Abstractions.Indexing.FieldStorage.Yes)
        definition.Stores.Add(aggregateTypeFieldName, Raven.Abstractions.Indexing.FieldStorage.Yes)
        definition.Stores.Add(streamIdFieldName, Raven.Abstractions.Indexing.FieldStorage.Yes)
        definition

    let deserialize (serializer :  ISerializer) (doc : RavenJObject) (propertyTypes : Map<string,Type>) =
        let deserializeRavenJToken targetType (jToken : RavenJToken) =
            jToken.ToString(Raven.Imports.Newtonsoft.Json.Formatting.None)
            |> System.Text.Encoding.UTF8.GetBytes
            |> (fun x -> serializer.DeserializeObj x targetType)

        let addKey stateMap key =
            let blockType = propertyTypes.Item key
            let jToken = doc.Item key
            let value = deserializeRavenJToken blockType jToken
            stateMap |> Map.add key value

        doc.Keys
        |> Seq.filter (fun x -> x <> "AggregateType")
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

    let getTickString (dateTime : DateTime) =
        dateTime.Ticks.ToString("D21")

    let fromTickString (str : String) =
        str 
        |> System.Int64.Parse
        |> (fun x -> new DateTime(x, DateTimeKind.Utc))

    let deserializeDateString (value : string) =
        match value with
        | null -> None
        | value -> 
            value 
            |> UtcDateTime.fromString
            |> Some

    let serializeDateTimeOption = function
        | None -> null
        | Some (dateTime : UtcDateTime) ->
            UtcDateTime.toString dateTime

    let getAggregateState
        (documentStore : Raven.Client.IDocumentStore) 
        serializer 
        (database : string) 
        streamId 
        typeMap
        = 
        async {
        let stateDocumentKey = getDocumentKey streamId
        use session = documentStore.OpenAsyncSession(database)
        let! doc = session.LoadAsync<AggregateStateDocument> stateDocumentKey |> Async.AwaitTask

        if box doc <> null then
            let snapshot = deserialize serializer doc.Snapshot typeMap
            return {
                AggregateState.Snapshot = { StateSnapshot.State =  snapshot; LastEventNumber = doc.LastEventNumber }
                NextWakeup = deserializeDateString doc.NextWakeup
            }
        else
            return {
                AggregateState.Snapshot = StateSnapshot.Empty
                NextWakeup = None
            }
    }

    let getStateSnapshot
        (documentStore : Raven.Client.IDocumentStore) 
        serializer 
        (database : string) 
        streamId 
        typeMap
        =
        getAggregateState documentStore serializer database streamId typeMap
        |> Async.map (fun x -> x.Snapshot)

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
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent>) =

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
            let typeMap = StateBuilder.getTypeMapFromStateBuilder aggregateConfig.StateBuilder

            let (snapshot : StateSnapshot, metadata : RavenJObject, etag) = 
                doc
                |> Option.map (fun (stateDocument : AggregateStateDocument, metadata : RavenJObject, etag) ->
                    ({ 
                        StateSnapshot.LastEventNumber = stateDocument.LastEventNumber
                        State = deserialize serializer stateDocument.Snapshot typeMap
                    }, metadata, etag)
                )
                |> Option.getOrElseF (fun () -> 
                    (StateSnapshot.Empty, emptyMetadata(), Raven.Abstractions.Data.Etag.Empty))

            let docMetadata = (documentKey, metadata, etag)
            return (persistedEvents, aggregateType, aggregateConfig, snapshot, docMetadata)
        }

        let applyEventsToSnapshot (persistedEvents : seq<PersistedEvent<'TMetadata>>, aggregateType : string, aggregateConfig : EventfulStreamConfig<'TMetadata>, snapshot, docMetadata) =
            let snapshot' = applyMessages aggregateConfig snapshot persistedEvents
            (persistedEvents, aggregateType, aggregateConfig, snapshot', docMetadata)

        let computeNextWakeup (persistedEvents, aggregateType, (aggregateConfig : EventfulStreamConfig<_>), snapshot, docMetadata) =
            let nextWakeup = 
                maybe {
                    let! EventfulWakeupHandler (nextWakeupStateBuilder,_) = aggregateConfig.Wakeup
                    return! nextWakeupStateBuilder.GetState snapshot.State
                }
            (persistedEvents, aggregateType, aggregateConfig, snapshot, docMetadata, nextWakeup)

        let createWriteRequest streamId (_, aggregateType, (aggregateConfig : EventfulStreamConfig<_>), (snapshot : StateSnapshot), (documentKey, metadata, etag), (nextWakeup:UtcDateTime option)) =
            let updatedDoc = {
                AggregateStateDocument.Snapshot = mapToRavenJObject serializer snapshot.State
                LastEventNumber = snapshot.LastEventNumber
                NextWakeup = serializeDateTimeOption nextWakeup
                AggregateType = aggregateType
                StreamId = streamId
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
                    >> (createWriteRequest streamId)
                    >> Seq.singleton
                    >> flip tuple2 (async.Zero())
                )
        {
            Projector.MatchingKeys = matchingKeys
            Projector.ProcessEvents = processEvents
        }