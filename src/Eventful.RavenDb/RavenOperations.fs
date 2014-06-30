namespace Eventful.Raven

open System
open System.Runtime.Caching
open Raven.Json.Linq
open Raven.Client

module RavenOperations =
    let getDocument (documentStore : IDocumentStore) (cache : MemoryCache) database docKey =
        let cacheEntry = cache.Get(database + "::" + docKey)
        match cacheEntry with
        | :? ProjectedDocument<_> as doc ->
            async { return Some doc }
        | _ -> 
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

    let emptyMetadata (entityName : string) = 
        let metadata = new Raven.Json.Linq.RavenJObject()
        metadata.Add("Raven-Entity-Name", new RavenJValue(entityName))
        metadata