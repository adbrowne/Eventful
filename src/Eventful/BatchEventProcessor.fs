namespace Eventful

open System
open System.Threading.Tasks

type BatchEventProcessor<'TKey, 'TMessage> = {
    MatchingKeys: 'TMessage -> seq<'TKey>
    /// returns a func so the task does not start immediately when using Async.StartAsTask
    Process: ('TKey * seq<'TMessage>) -> Func<Task<Choice<unit,exn>>>
}
