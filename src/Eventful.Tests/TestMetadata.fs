namespace Eventful.Tests

open System

type TestMetadata = {
    MessageId : Guid
    SourceMessageId : String
    AggregateId : Guid
}