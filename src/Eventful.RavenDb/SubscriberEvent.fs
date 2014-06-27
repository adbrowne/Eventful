namespace Eventful.Raven

type SubscriberEvent<'TContext> = {
    Event : obj
    Context : 'TContext
    StreamId : string
    EventNumber: int
}