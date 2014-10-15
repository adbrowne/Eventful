namespace Eventful

open System
open FSharpx
open Eventful
open Eventful.EventStream

type accumulator<'TState,'TItem> = 'TState -> 'TItem -> 'TState

type IdMapper<'TId>(handlers : List<(obj -> 'TId option)>, types : List<Type>) =
    member x.Types = types
    member x.Run (item: obj) =
        handlers
        |> List.map (fun h -> h item)
        |> Seq.find (fun e -> match e with
                              | Some _ -> true
                              | None -> false)
        |> (fun v -> v.Value)

    member x.AddHandler<'T> (f: 'T -> 'TId) =
        let msgType = typeof<'T>
        let func (message : obj) =
            match message with
            | :? 'T as msg -> Some (f msg)
            | _ -> None
        new IdMapper<'TId>(func::handlers, (typeof<'T>)::types)
    static member Empty = new IdMapper<'TId>(List.empty, List.empty)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module IdMapper =
    let addHandler<'TId, 'TEvent> (f : 'TEvent -> 'TId) (b : IdMapper<'TId>) =
        b.AddHandler f