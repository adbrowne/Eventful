namespace Eventful.Testing

open FSharpx.Collections
open Eventful
open System

type TestMetadata = {
    MessageId : Guid
    SourceMessageId : String
    AggregateId : Guid
}

type TestEventStore<'TMetadata when 'TMetadata : equality> = {
    Position : EventPosition
    Events : Map<string,Vector<EventPosition * EventStreamEvent<'TMetadata>>>
    AllEventsStream : Queue<string * int * EventStreamEvent<'TMetadata>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let nextPosition (position : EventPosition) = 
        {
            Commit = position.Commit + 1L
            Prepare = position.Commit + 1L
        }
    let empty<'TMetadata when 'TMetadata : equality> : TestEventStore<'TMetadata> = { Position = EventPosition.Start; Events = Map.empty; AllEventsStream = Queue.empty }

    let addEvent stream (streamEvent: EventStreamEvent<'TMetadata>) (store : TestEventStore<'TMetadata>) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let eventPosition = nextPosition store.Position
        let eventNumber = streamEvents.Length
        let streamEvents' = streamEvents |> Vector.conj (eventPosition, streamEvent)
        { store with 
            Events = store.Events |> Map.add stream streamEvents'; 
            Position = eventPosition 
            AllEventsStream = store.AllEventsStream |> Queue.conj (stream, eventNumber, streamEvent)}

    let runHandlerForEvent (context: 'TEventContext) interpreter (eventStream, eventNumber, evt) testEventStore (EventfulEventHandler (t, evtHandler)) =
        let program = evtHandler context eventStream eventNumber evt
        interpreter program testEventStore
        |> fst

    let runEventHandlers (context: 'TEventContext) interpreter (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata>) (testEventStore : TestEventStore<'TMetadata>) (eventStream, eventNumber, eventStreamEvent) =
        match eventStreamEvent with
        | Event { Body = evt; EventType = eventType; Metadata = metadata } ->
            let handlers = 
                handlers.EventHandlers
                |> Map.tryFind (evt.GetType().Name)
                |> function
                | Some handlers -> handlers
                | None -> []

            handlers |> Seq.fold (runHandlerForEvent context interpreter (eventStream, eventNumber, { Body = evt; EventType = eventType; Metadata = metadata })) testEventStore
        | _ -> testEventStore

    let rec processPendingEvents (context: 'TEventContext) interpreter (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata>) (testEventStore : TestEventStore<'TMetadata>) =
        match testEventStore.AllEventsStream with
        | Queue.Nil -> testEventStore
        | Queue.Cons (x, xs) ->
            let next = runEventHandlers context interpreter handlers { testEventStore with AllEventsStream = xs } x
            processPendingEvents context interpreter handlers next

    let runCommand interpreter cmd handler testEventStore =
        let program = handler cmd
        interpreter program testEventStore