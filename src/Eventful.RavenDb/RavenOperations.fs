namespace Eventful.Raven

open System
open System.Runtime.Caching
open Raven.Json.Linq
open Raven.Client
open Raven.Abstractions.Data

module RavenOperations =
    let log = Common.Logging.LogManager.GetLogger(typeof<BatchWrite>)

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

    let getDocuments (documentStore : IDocumentStore) (cache : MemoryCache) (database : string) (request : seq<string * Type>) = async {
        let requestCacheMatches =
            request
            |> Seq.map(fun (docKey, docType) -> 
                let cacheEntry = getCacheKey database docKey |> cache.Get
                match cacheEntry with
                | null -> (docKey, docType, None)
                | :? (obj * RavenJObject * Etag) as value -> 
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
            |> Seq.collect (function 
                | null -> Seq.empty
                | jsonDoc ->
                    let docKey = jsonDoc.Key
                    let docType = fetchTypes |> Map.find docKey
                    let actualDoc = deserializeToType documentStore docType jsonDoc.DataAsJson
                    Seq.singleton (docKey, (actualDoc, jsonDoc.Metadata, jsonDoc.Etag)))
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
        let metadata = new RavenJObject()
        metadata.Add("Raven-Entity-Name", new RavenJValue(entityName))
        metadata.Add("Raven-Clr-Type", new RavenJValue(documentType.FullName))
        metadata

    let emptyMetadata<'T> (documentStore : IDocumentStore) = 
        emptyMetadataForType documentStore typeof<'T>