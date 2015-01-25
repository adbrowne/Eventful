namespace BookLibrary

open System 
open Eventful

type EventStoreMessage = {
    EventContext : BookLibraryEventMetadata
    Id : Guid
    Event : obj
    StreamIndex : int
    EventPosition : EventPosition
    StreamName : string
    EventType : string
} with
    static member ToPersitedEvent x = {
        PersistedEvent.Body = x.Event
        StreamId = x.StreamName
        EventNumber = x.StreamIndex
        EventId = x.Id
        EventType = x.EventType
        Metadata = x.EventContext }

    interface IBulkMessage with
        member x.GlobalPosition = 
            Some x.EventPosition
        member x.EventType =
            x.Event.GetType()