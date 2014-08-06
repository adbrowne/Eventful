namespace Eventful.Raven

open System
open System.Runtime.Caching
open Raven.Json.Linq
open Raven.Client
open Raven.Abstractions.Data

type GetDocRequest = string * Type 
type GetDocResponse = string * Type * (obj * RavenJObject * Etag) option

open FSharpx.Option
open FSharpx

module RavenOperations =
    let log = Common.Logging.LogManager.GetLogger(typeof<BatchWrite>)

    let cacheHitCounter = Metrics.Metric.Meter("RavenOperations Cache Hit", Metrics.Unit.Items)

    let serializeDocument<'T> (documentStore : IDocumentStore) (doc : 'T) =
        let serializer = documentStore.Conventions.CreateSerializer()
        RavenJObject.FromObject(doc, serializer)

    let deserializeToType (documentStore : IDocumentStore) (toType : Type) ravenJObject =
        let serializer = documentStore.Conventions.CreateSerializer()
        serializer.Deserialize(new RavenJTokenReader(ravenJObject), toType)

    let deserialize<'T> (documentStore : IDocumentStore) ravenJObject =
        deserializeToType documentStore typeof<'T> ravenJObject

    let getCacheKey databaseName docKey = databaseName + "::" + docKey 

    let getDocument<'TDocument> (documentStore : IDocumentStore) (cache : MemoryCache) database docKey =
        let cacheKey = getCacheKey database docKey 
        let cacheEntry = cache.Get cacheKey
        match cacheEntry with
        | :? ProjectedDocument<obj> as projectedDoc ->
            let (doc, metadata, etag) = projectedDoc
            //cacheHitCounter.Mark()
            async { return Some (doc :?> 'TDocument, metadata, etag) }
        | null -> 
            async {
                use session = documentStore.OpenAsyncSession(database)
                let! doc = session.LoadAsync<_>(docKey) |> Async.AwaitTask
                if Object.Equals(doc, null) then
                    return None
                else
                    let etag = session.Advanced.GetEtagFor(doc)
                    let metadata = session.Advanced.GetMetadataFor(doc)
                    return Some (doc, metadata, etag)
            }
        | entry -> failwith <| sprintf "Unexpected entry type %A" entry

    let getDocuments (documentStore : IDocumentStore) (cache : MemoryCache) (database : string) (request : seq<GetDocRequest>) : Async<seq<GetDocResponse>> = async {
        let requestCacheMatches =
            request
            |> Seq.map(fun (docKey, docType) -> 
                let cacheEntry = getCacheKey database docKey |> cache.Get
                match cacheEntry with
                | null -> (docKey, docType, None)
                | :? (obj * RavenJObject * Etag) as value -> 
                    //cacheHitCounter.Mark()
                    let (doc, metadata, etag) = value
                    (docKey, docType, Some (doc, metadata, etag))
                | a -> failwith <| sprintf "Unexpected %A" a)

        let toFetch =
            requestCacheMatches
            |> Seq.collect (function
                | docKey, _, None -> Seq.singleton docKey
                | _ -> Seq.empty)
            |> Array.ofSeq

        let fetchTypes = 
             requestCacheMatches
            |> Seq.collect (function
                | docKey, docType, None -> Seq.singleton (docKey, docType)
                | _ -> Seq.empty)
            |> Map.ofSeq

        let commands = documentStore.AsyncDatabaseCommands.ForDatabase(database)
        let! rawDocs = commands.GetAsync(toFetch, Array.empty) |> Async.AwaitTask

        let serializer = documentStore.Conventions.CreateSerializer()

        let rawDocMap =
            rawDocs.Results
            |> Raven.Client.Connection.SerializationHelper.ToJsonDocuments
            |> Seq.choose (function 
                | null -> None
                | jsonDoc ->
                    maybe {
                        let docKey = jsonDoc.Key
                        let! docType = fetchTypes |> Map.tryFind docKey
                        let actualDoc = deserializeToType documentStore docType jsonDoc.DataAsJson
                        return (docKey, (actualDoc, jsonDoc.Metadata, jsonDoc.Etag))
                    })
            |> Map.ofSeq

        return 
            requestCacheMatches
            |> Seq.map (function
                | docKey, docType, None ->
                    (docKey, docType, rawDocMap |> Map.tryFind docKey)
                | x -> x)
    }

    let emptyMetadataForType (documentStore : IDocumentStore) (documentType : Type) = 
        let entityName = documentStore.Conventions.GetTypeTagName(documentType)
        let clrTypeName = documentStore.Conventions.GetClrTypeName(documentType);

        let metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase)
        metadata.Add("Raven-Entity-Name", new RavenJValue(entityName))
        metadata.Add("Raven-Clr-Type", new RavenJValue(clrTypeName))
        metadata

    let emptyMetadata<'T> (documentStore : IDocumentStore) = 
        emptyMetadataForType documentStore typeof<'T>