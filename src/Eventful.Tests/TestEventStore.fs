namespace Eventful.Testing

open FSharpx.Collections
open Eventful

type TestEventStore = {
    Events : Map<string,Vector<EventStreamEvent>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let empty : TestEventStore = { Events = Map.empty }
    let addEvent (stream, event, metadata) (store : TestEventStore) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let streamEvents' = streamEvents |> Vector.conj (Event (event, metadata))
        { store with Events = store.Events |> Map.add stream streamEvents' }