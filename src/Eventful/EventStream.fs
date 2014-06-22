namespace Eventful

open System

type EventMetadata = {
    MessageId : Guid
    SourceMessageId : Guid
}

type WriteResult =
| WriteSuccess
| WrongExpectedVersion
| WriteCancelled
| WriteError of System.Exception

type EventStreamEvent = 
| Event of (obj * EventMetadata)
| EventLink of (string * int * EventMetadata)

module EventStream =
    type EventToken = {
        Stream : string
        Number : int
        EventType : string
    }

    type EventStreamLanguage<'N> =
    | ReadFromStream of string * int * (EventToken option -> 'N)
    | ReadValue of EventToken * Type * (obj -> 'N)
    | WriteToStream of string * int * seq<EventStreamEvent> * (WriteResult -> 'N)
    | NotYetDone of (unit -> 'N)

    let fmap f streamWorker = 
        match streamWorker with
        | ReadFromStream (stream, number, streamRead) -> 
            ReadFromStream (stream, number, (streamRead >> f))
        | ReadValue (eventToken, eventType, readValue) -> 
            ReadValue (eventToken, eventType, readValue >> f)
        | WriteToStream (stream, expectedVersion, events, next) -> 
            WriteToStream (stream, expectedVersion, events, (next >> f))
        | NotYetDone (delay) ->
            NotYetDone (fun () -> f (delay()))

    type FreeEventStream<'F,'R> = 
        | FreeEventStream of EventStreamLanguage<FreeEventStream<'F,'R>>
        | Pure of 'R

    let empty = Pure ()

    // liftF :: (Functor f) => f r -> Free f r -- haskell signature
    let liftF command = FreeEventStream (fmap Pure command)

    let readFromStream stream number = 
        ReadFromStream (stream, number, id) |> liftF
    let readValue eventToken eventType = 
        ReadValue(eventToken, eventType, id) |> liftF
    let writeToStream stream number events = 
        WriteToStream(stream, number, events, id) |> liftF

    let rec bind f v =
        match v with
        | FreeEventStream x -> FreeEventStream (fmap (bind f) x)
        | Pure r -> f r

    // Return the final value wrapped in the Free type.
    let result value = Pure value

    // The whileLoop operator.
    // This is boilerplate in terms of "result" and "bind".
    let rec whileLoop pred body =
        if pred() then body |> bind (fun _ -> whileLoop pred body)
        else result ()

    // The delay operator.
    let delay (func : unit -> FreeEventStream<'a,'b>) : FreeEventStream<'a,'b> = 
        let notYetDone = NotYetDone (fun () -> ()) |> liftF
        bind func notYetDone

    // The sequential composition operator.
    // This is boilerplate in terms of "result" and "bind".
    let combine expr1 expr2 =
        expr1 |> bind (fun () -> expr2)

    // The forLoop operator.
    // This is boilerplate in terms of "catch", "result", and "bind".
    let forLoop (collection:seq<_>) func =
        let ie = collection.GetEnumerator()
        (whileLoop (fun () -> ie.MoveNext())
            (delay (fun () -> let value = ie.Current in func value)))

    type EventStreamBuilder() =
        member x.Zero() = Pure ()
        member x.Return(r:'R) : FreeEventStream<'F,'R> = Pure r
        member x.ReturnFrom(r:FreeEventStream<'F,'R>) : FreeEventStream<'F,'R> = r
        member x.Bind (inp : FreeEventStream<'F,'R>, body : ('R -> FreeEventStream<'F,'U>)) : FreeEventStream<'F,'U>  = bind body inp
        member x.Combine(expr1, expr2) = combine expr1 expr2
        member x.For(a, f) = forLoop a f 
        member x.Delay(func) = delay func

    let eventStream = new EventStreamBuilder()

    type EventStreamProgram<'A> = FreeEventStream<obj,'A>


    // Higher level eventstream operations

    let writeLink stream expectedVersion linkStream linkEventNumber metadata =
        let writes : seq<EventStreamEvent> = Seq.singleton (EventStreamEvent.EventLink(linkStream, linkEventNumber, metadata))
        writeToStream stream expectedVersion writes