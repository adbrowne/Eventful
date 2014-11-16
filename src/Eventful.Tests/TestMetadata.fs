namespace Eventful.Tests

open System
open Eventful

type TestMetadata = {
    MessageId : Guid
    SourceMessageId : String
    AggregateId : Guid
    AggregateType : string
}
with 
    static member GetUniqueId x = Some x.SourceMessageId
    static member GetAggregateId x = x.AggregateId
    static member GetAggregateType x = x.AggregateType

type TestAggregateDefinition<'TCommandContext,'TEventContext> = AggregateDefinition<Guid, 'TCommandContext, 'TEventContext, TestMetadata,IEvent, string>