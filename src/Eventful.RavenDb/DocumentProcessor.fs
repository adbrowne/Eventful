namespace Eventful.Raven

open System

open Raven.Abstractions.Data
open Raven.Json.Linq

type DocumentProcessor<'TKey, 'TDocument, 'TContext> = {
    GetDocumentKey : 'TKey -> string
    GetPermDocumentKey : 'TKey -> string
    EventTypes : seq<Type>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<'TKey>
    Process: 'TKey -> IDocumentFetcher -> seq<SubscriberEvent<'TContext>> -> Async<seq<DocumentWriteRequest>>
    NewDocument : 'TKey -> ('TDocument * RavenJObject * Etag)
    BeforeWrite : ('TDocument * RavenJObject * Etag) -> ('TDocument * RavenJObject * Etag)
}