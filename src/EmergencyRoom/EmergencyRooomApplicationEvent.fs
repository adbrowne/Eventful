namespace EmergencyRoom

open System 
open Eventful

type EventStoreMessage = {
    EventContext : EmergencyEventMetadata
    Id : Guid
    Event : obj
    StreamIndex : int
    EventPosition : EventPosition
    StreamName : string
} with
    interface IBulkRavenMessage with
        member x.GlobalPosition = 
            Some x.EventPosition
        member x.EventType =
            x.Event.GetType()