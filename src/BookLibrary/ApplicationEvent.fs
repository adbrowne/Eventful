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
} with
    interface IBulkMessage with
        member x.GlobalPosition = 
            Some x.EventPosition
        member x.EventType =
            x.Event.GetType()