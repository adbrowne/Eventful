namespace Eventful.Raven

open System
open Eventful
open Raven.Abstractions.Data
open Raven.Json.Linq

type DocumentProcessor<'TKey, 'TDocument, 'TMessage> = {
    EventTypes : seq<Type>
    MatchingKeys: 'TMessage -> seq<'TKey>
    Process: 'TKey -> IDocumentFetcher -> seq<'TMessage> -> Async<seq<ProcessAction>>
}