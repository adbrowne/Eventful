namespace Eventful.Testing

open FSharpx.Collections
open Eventful

type TestEventStore = {
    Events : Map<string,Vector<EventStreamEvent>>
    AllEventsStream : Queue<string * int * EventStreamEvent>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let empty : TestEventStore = { Events = Map.empty; AllEventsStream = Queue.empty }

    let addEvent (stream, event, metadata) (store : TestEventStore) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let streamEvents' = streamEvents |> Vector.conj (Event (event, metadata))
        { store with Events = store.Events |> Map.add stream streamEvents' }

    let runHandlerForEvent interpreter (eventStream, eventNumber, evt) testEventStore (EventfulEventHandler (t, evtHandler)) =
        let program = evtHandler eventStream eventNumber evt
        interpreter program testEventStore
        |> fst

    let runEventHandlers interpreter (handlers : EventfulHandlers) (testEventStore : TestEventStore) (eventStream, eventNumber, eventStreamEvent) =
        match eventStreamEvent with
        | Event (evt, metadata) ->
            let handlers = 
                handlers.EventHandlers
                |> Map.tryFind (evt.GetType().Name)
                |> function
                | Some handlers -> handlers
                | None -> []

            handlers |> Seq.fold (runHandlerForEvent interpreter (eventStream, eventNumber, evt)) testEventStore
        | _ -> testEventStore

    let rec processPendingEvents interpreter (handlers : EventfulHandlers) (testEventStore : TestEventStore) =
        match testEventStore.AllEventsStream with
        | Queue.Nil -> testEventStore
        | Queue.Cons (x, xs) ->
            let next = runEventHandlers interpreter handlers { testEventStore with AllEventsStream = xs } x
            processPendingEvents interpreter handlers next

    let runCommand interpreter cmd handler testEventStore =
        let program = handler cmd
        interpreter program testEventStore