namespace Eventful

type EventPosition = {
    Commit: int64
    Prepare : int64
}

type SubscriberEvent<'TContext> = {
    Event : obj
    Context : 'TContext
    StreamId : string
    EventNumber: int
}