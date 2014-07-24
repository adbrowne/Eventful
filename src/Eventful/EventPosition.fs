namespace Eventful

type EventPosition = {
    Commit: int64
    Prepare : int64
}
with static member Start = { Commit = 0L; Prepare = 0L }

type IBulkRavenMessage = 
    abstract member GlobalPosition : EventPosition option
    abstract member EventType : System.Type