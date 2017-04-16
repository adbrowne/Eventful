﻿namespace Eventful.Raven

open System
open System.Runtime.Caching
open Raven.Json.Linq
open Raven.Client
open Raven.Abstractions.Data

open Eventful

type RavenMemoryCache(cacheName : string, documentStore : Raven.Client.IDocumentStore) =
    let cache = new MemoryCache(cacheName)

//    let cacheHitCounter = Metrics.Metric.Meter("RavenOperations Cache Hit", Metrics.Unit.Items)

    let getCacheKey databaseName docKey = databaseName + "::" + docKey

    let cloneEntry (document : obj, metadata : RavenJObject, etag : Etag) =
        let serializer = documentStore.Conventions.CreateSerializer() 
        let documentJson = RavenJObject.FromObject(document, serializer)
        use documentReader = new RavenJTokenReader(documentJson)
        let clonedDocument = serializer.Deserialize(documentReader, document.GetType())
        let clonedMetadata = metadata.CloneToken() :?> RavenJObject
        
        (clonedDocument, clonedMetadata, etag)

    member this.Get mode databaseName docKey =
        let cacheKey = getCacheKey databaseName docKey

        match cache.Get cacheKey with
        | :? (obj * RavenJObject * Etag) as cacheHit ->
            //cacheHitCounter.Mark()

            match mode with
            | AccessMode.Read ->
                Some cacheHit
                
            | AccessMode.Update ->
                // Clone the result to avoid other readers seeing a partially updated object.
                Some (cloneEntry cacheHit)

        | null -> None
        | cacheEntry -> failwith <| sprintf "Unexpected entry type %A" cacheEntry

    member this.GetForRead databaseName docKey =
        this.Get AccessMode.Read databaseName docKey

    member this.GetForUpdate databaseName docKey =
        this.Get AccessMode.Update databaseName docKey

    member this.Set databaseName docKey (value : obj * RavenJObject * Etag) =
        let cacheKey = getCacheKey databaseName docKey
        cache.Set(cacheKey, value, DateTimeOffset.MaxValue)

    member this.Remove databaseName docKey =
        let cacheKey = getCacheKey databaseName docKey
        cache.Remove cacheKey |> ignore

    interface IDisposable with
        member this.Dispose() = cache.Dispose()


type GetDocResponse = string * Type * (obj * RavenJObject * Etag) option

open FSharpx.Option
open FSharpx

module RavenOperations =
    let log = createLogger "Eventful.Raven.RavenOperations"

    let serializeDocument<'T> (documentStore : IDocumentStore) (doc : 'T) =
        let serializer = documentStore.Conventions.CreateSerializer()
        RavenJObject.FromObject(doc, serializer)

    let deserializeToType (documentStore : IDocumentStore) (toType : Type) ravenJObject =
        let serializer = documentStore.Conventions.CreateSerializer()
        serializer.Deserialize(new RavenJTokenReader(ravenJObject), toType)

    let deserialize<'T> (documentStore : IDocumentStore) ravenJObject =
        deserializeToType documentStore typeof<'T> ravenJObject

    let writeDoc (documentStore : Raven.Client.IDocumentStore) database (key : string) (doc : obj) (metadata : RavenJObject) = async {
        use commands = documentStore.AsyncDatabaseCommands.ForDatabase(database)
        let jsonDoc = RavenJObject.FromObject(doc, documentStore.Conventions.CreateSerializer())
        do! commands.PutAsync(key, null, jsonDoc, metadata) |> Async.AwaitTask |> Async.Ignore
    }

    let getDocuments (documentStore : IDocumentStore) (cache : RavenMemoryCache) (database : string) (requests : seq<GetDocRequest>) : Async<seq<GetDocResponse>> = async {
        let requestsWithCacheMatches =
            requests
            |> Seq.map (fun request -> 
                let cacheEntry = cache.Get request.AccessMode database request.DocumentKey
                (request.DocumentKey, request.DocumentType, cacheEntry))
            |> List.ofSeq

        let keysToFetch =
            requestsWithCacheMatches
            |> Seq.choose (fun (documentKey, _, cacheEntry) ->
                match cacheEntry with
                | None -> Some documentKey
                | _ -> None)
            |> Set.ofSeq
            |> Array.ofSeq

        let commands = documentStore.AsyncDatabaseCommands.ForDatabase(database)
        let! fetchedDocuments = commands.GetAsync(keysToFetch, includes = Array.empty) |> Async.AwaitTask

        let jsonDocuments =
            fetchedDocuments.Results
            |> Raven.Client.Connection.SerializationHelper.ToJsonDocuments
            |> Seq.choose (function
                | null -> None
                | document -> Some (document.Key, document))
            |> Map.ofSeq

        let getJsonDocument (documentKey : string) (documentType : Type) =
            maybe {
                let! jsonDocument = jsonDocuments |> Map.tryFind documentKey
                let actualDoc = deserializeToType documentStore documentType jsonDocument.DataAsJson
                return (actualDoc, jsonDocument.Metadata.CloneToken() :?> RavenJObject, jsonDocument.Etag)
            }

        let mergeCachedAndFetched (documentKey, documentType, cacheResult) =
            let result =
                match cacheResult with
                | None -> getJsonDocument documentKey documentType
                | cacheHit -> cacheHit

            (documentKey, documentType, result)

        return Seq.map mergeCachedAndFetched requestsWithCacheMatches
    }

    let emptyMetadataForType (documentStore : IDocumentStore) (documentType : Type) = 
        let entityName = documentStore.Conventions.GetTypeTagName(documentType)
        let clrTypeName = documentStore.Conventions.GetClrTypeName(documentType);

        let metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase)
        metadata.Add("Raven-Entity-Name", new RavenJValue(entityName + "s"))
        metadata.Add("Raven-Clr-Type", new RavenJValue(clrTypeName))
        metadata

    let emptyMetadata<'T> (documentStore : IDocumentStore) = 
        emptyMetadataForType documentStore typeof<'T>
