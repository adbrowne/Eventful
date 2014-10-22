namespace Eventful

open System

type ExpectedAggregateVersion =
| Any
| NewStream
| AggregateVersion of int

type WriteResult =
| WriteSuccess of EventPosition
| WrongExpectedVersion
| WriteCancelled
| WriteError of System.Exception

type EventStreamEventData<'TMetadata> = {
    Body : obj
    EventType : string
    Metadata : 'TMetadata
}

type EventTypeMap = Bimap<string, ComparableType>

type EventStreamEvent<'TMetadata> = 
| Event of (EventStreamEventData<'TMetadata>)
| EventLink of (string * int * 'TMetadata)

module EventStream =
    open FSharpx.Operators
    open FSharpx.Collections

    type EventToken = {
        Stream : string
        Number : int
        EventType : string
    }

    type EventStreamLanguage<'N,'TMetadata> =
    | ReadFromStream of string * int * (EventToken option -> 'N)
    | GetEventTypeMap of unit * (EventTypeMap -> 'N)
    | ReadValue of EventToken * ((obj * 'TMetadata) -> 'N)
    | WriteToStream of string * ExpectedAggregateVersion * seq<EventStreamEvent<'TMetadata>> * (WriteResult -> 'N)
    | NotYetDone of (unit -> 'N)

    let fmap f streamWorker = 
        match streamWorker with
        | ReadFromStream (stream, number, streamRead) -> 
            ReadFromStream (stream, number, (streamRead >> f))
        | GetEventTypeMap (eventTypeMap,next) -> 
            GetEventTypeMap (eventTypeMap, next >> f)
        | ReadValue (eventToken, readValue) -> 
            ReadValue (eventToken, readValue >> f)
        | WriteToStream (stream, expectedVersion, events, next) -> 
            WriteToStream (stream, expectedVersion, events, (next >> f))
        | NotYetDone (delay) ->
            NotYetDone (fun () -> f (delay()))

    type FreeEventStream<'F,'R,'TMetadata> = 
        | FreeEventStream of EventStreamLanguage<FreeEventStream<'F,'R,'TMetadata>,'TMetadata>
        | Pure of 'R

    let empty = Pure ()

    // liftF :: (Functor f) => f r -> Free f r -- haskell signature
    let liftF command = FreeEventStream (fmap Pure command)

    let readFromStream stream number = 
        ReadFromStream (stream, number, id) |> liftF
    let getEventTypeMap unit =
        GetEventTypeMap ((), id) |> liftF
    let readValue eventToken = 
        ReadValue(eventToken, id) |> liftF
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
    let delay (func : unit -> FreeEventStream<'a,'b,'TMetadata>) : FreeEventStream<'a,'b,'TMetadata> = 
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
        member x.Return(r:'R) : FreeEventStream<'F,'R,'TMetadata> = Pure r
        member x.ReturnFrom(r:FreeEventStream<'F,'R,'TMetadata>) : FreeEventStream<'F,'R,'TMetadata> = r
        member x.Bind (inp : FreeEventStream<'F,'R,'TMetadata>, body : ('R -> FreeEventStream<'F,'U,'TMetadata>)) : FreeEventStream<'F,'U,'TMetadata>  = bind body inp
        member x.Combine(expr1, expr2) = combine expr1 expr2
        member x.For(a, f) = forLoop a f 
        member x.While(func, body) = whileLoop func body
        member x.Delay(func) = delay func

    let eventStream = new EventStreamBuilder()

    type EventStreamProgram<'A,'TMetadata> = FreeEventStream<obj,'A,'TMetadata>

    let inline returnM x = returnM eventStream x

    let inline (<*>) f m = applyM eventStream eventStream f m

    let inline lift2 f x y = returnM f <*> x <*> y

    let inline sequence s =
        let inline cons a b = lift2 List.cons a b
        List.foldBack cons s (returnM [])

    let inline mapM f x = sequence (List.map f x)

    // Higher level eventstream operations

    let writeLink stream expectedVersion linkStream linkEventNumber metadata =
        let writes : seq<EventStreamEvent<'TMetadata>> = Seq.singleton (EventStreamEvent.EventLink(linkStream, linkEventNumber, metadata))
        writeToStream stream expectedVersion writes

    let getEventStreamEvent evt metadata = eventStream {
        let! eventTypeMap = getEventTypeMap ()
        let eventType = eventTypeMap.FindValue (new ComparableType(evt.GetType()))
        return EventStreamEvent.Event { Body = evt :> obj; EventType = eventType; Metadata = metadata }
    }