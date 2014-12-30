namespace Eventful.Tests

open System
open Eventful

type TestMetadata = {
    SourceMessageId : String option
    AggregateType : string
}
with 
    static member GetUniqueId x = x.SourceMessageId
    static member GetAggregateType x = x.AggregateType

type TestAggregateDefinition<'TCommandContext,'TEventContext> = AggregateDefinition<Guid, 'TCommandContext, 'TEventContext, TestMetadata,IEvent, string>