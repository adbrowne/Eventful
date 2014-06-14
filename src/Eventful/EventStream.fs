namespace Eventful

open System

module EventStream =
    type EventToken = {
        Stream : string
        Number : int
        EventType : string
    }

    type EventStreamLanguage<'N> =
    | ReadFromStream of string * int * (EventToken option -> 'N)
    | ReadValue of EventToken * Type * (obj -> 'N)
    | WriteToStream of string * int * 'N

    let fmap f streamWorker = 
        match streamWorker with
        | ReadFromStream (stream, number, streamRead) -> 
            ReadFromStream (stream, number, (streamRead >> f))
        | ReadValue (eventToken, eventType, readValue) -> 
            ReadValue (eventToken, eventType, readValue >> f)
        | WriteToStream (stream, expectedVersion, next) -> 
            WriteToStream (stream, expectedVersion, (f next))

    type FreeEventStream<'F,'R> = 
        | FreeEventStream of EventStreamLanguage<FreeEventStream<'F,'R>>
        | Pure of 'R

    let empty = Pure ()

    type EventStreamProgram<'A> = FreeEventStream<obj,'A>

    // liftF :: (Functor f) => f r -> Free f r -- haskell signature
    let liftF command = FreeEventStream (fmap Pure command)

    let readFromStream stream number = 
        ReadFromStream (stream, number, id) |> liftF
    let readValue eventToken eventType = 
        ReadValue(eventToken, eventType, id) |> liftF
    let writeToStream stream number = 
        WriteToStream(stream, number, ()) |> liftF

    let rec bind f v =
        match v with
        | FreeEventStream x -> FreeEventStream (fmap (bind f) x)
        | Pure r -> f r

    type EventStreamBuilder() =
        member x.Zero() = Pure ()
        member x.Return(r:'R) : FreeEventStream<'F,'R> = Pure r
        member x.ReturnFrom(r:FreeEventStream<'F,'R>) : FreeEventStream<'F,'R> = r
        member x.Bind (inp : FreeEventStream<'F,'R>, body : ('R -> FreeEventStream<'F,'U>)) : FreeEventStream<'F,'U>  = bind body inp

    let eventStream = new EventStreamBuilder()