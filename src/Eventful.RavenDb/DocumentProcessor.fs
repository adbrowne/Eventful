namespace Eventful.Raven

open System
open Eventful
open Raven.Abstractions.Data
open Raven.Json.Linq
open System.Threading.Tasks

type DocumentProcessor<'TKey, 'TMessage> = {
    MatchingKeys: 'TMessage -> seq<'TKey>
    /// returns a func so the task does not start immediately when using Async.StartAsTask
    Process: ('TKey * IDocumentFetcher * seq<'TMessage>) -> Func<Task<seq<ProcessAction>>>
}