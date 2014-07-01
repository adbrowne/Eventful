namespace Eventful.Raven

open System
open Eventful
open Raven.Abstractions.Data
open Raven.Json.Linq

type DocumentProcessor<'TKey, 'TDocument, 'TContext> = {
    EventTypes : seq<Type>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<'TKey>
    Process: 'TKey -> IDocumentFetcher -> seq<SubscriberEvent<'TContext>> -> Async<seq<DocumentWriteRequest>>
}