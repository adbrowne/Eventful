namespace Eventful

type EventPosition = {
    Commit: int64
    Prepare : int64
}

type IBulkRavenMessage = 
    abstract member GlobalPosition : EventPosition option
    abstract member EventType : System.Type