namespace Eventful.Raven

open System
open Eventful
open Raven.Abstractions.Data
open Raven.Json.Linq

[<CustomEquality; CustomComparison>]
type KeyProcessor<'TKey,'TContext when 'TKey : equality and 'TKey : comparison> = {
    Key : 'TKey
    Process : IDocumentFetcher -> seq<SubscriberEvent<'TContext>> -> Async<seq<DocumentWriteRequest>>
}
with
    static member KeyValue p = 
        let {Key = key } = p
        key
    override x.Equals(y) = 
        equalsOn KeyProcessor<'TKey,'TContext>.KeyValue x y
    override x.GetHashCode() = 
        hashOn KeyProcessor<'TKey,'TContext>.KeyValue x
    interface System.IComparable with 
        member x.CompareTo y = compareOn KeyProcessor<'TKey,'TContext>.KeyValue x y

type DocumentProcessor<'TKey, 'TDocument, 'TContext when 'TKey : comparison> = {
    EventTypes : seq<Type>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<KeyProcessor<'TKey, 'TContext>>
}