namespace Eventful.Tests

open System
open Eventful

type TestMetadata = {
    SourceMessageId : String
    AggregateType : string
}
with 
    static member GetUniqueId x = Some x.SourceMessageId
    static member GetAggregateType x = x.AggregateType

type TestAggregateDefinition<'TCommandContext,'TEventContext> = AggregateDefinition<Guid, 'TCommandContext, 'TEventContext, TestMetadata,IEvent, string>