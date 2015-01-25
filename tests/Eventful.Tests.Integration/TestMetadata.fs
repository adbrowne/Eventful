namespace Eventful.Tests.Integration

open System
open Eventful

type IEvent = interface end

type TestMetadata = {
    SourceMessageId : String option
    AggregateType : string
}
with 
    static member GetUniqueId x = x.SourceMessageId
    static member GetAggregateType x = x.AggregateType

type TestAggregateDefinition<'TCommandContext,'TEventContext> = AggregateDefinition<Guid, 'TCommandContext, 'TEventContext, TestMetadata,IEvent>