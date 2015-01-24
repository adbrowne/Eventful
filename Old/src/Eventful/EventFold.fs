namespace Eventful

open System
open FSharpx
open Eventful

type HandlerFunction<'TState, 'TMetadata, 'TEvent> = 'TState * 'TEvent * 'TMetadata -> 'TState

type GetAllEventsKey<'TMetadata, 'TKey> = 'TMetadata -> 'TKey
type GetEventKey<'TMetadata, 'TEvent, 'TKey> = 'TEvent -> 'TMetadata -> 'TKey

type StateBuilderHandler<'TState, 'TMetadata, 'TKey> = 
    | AllEvents of GetAllEventsKey<'TMetadata, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>
    | SingleEvent of Type * GetEventKey<'TMetadata, obj, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>

type EventFold<'TState, 'TMetadata, 'TKey> 
    (
        initialState : 'TState, 
        handlers : StateBuilderHandler<'TState, 'TMetadata, 'TKey> list
    ) = 
    static member Empty initialState = new EventFold<'TState, 'TMetadata, 'TKey>(initialState, List.empty)

    member x.InitialState = initialState

    member x.Handlers = handlers

    member x.AddHandler<'T> (h:StateBuilderHandler<'TState, 'TMetadata, 'TKey>) =
        new EventFold<'TState, 'TMetadata, 'TKey>(initialState, h::handlers)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventFold = 
    let untypedHandler f (state, (evt : obj), metadata) = 
        match evt with
        | :? 'TEvent as evt ->
            f (state, evt, metadata) 
        | _ -> failwith <| sprintf "Expecting type: %s but got type: %s" typeof<'TEvent>.FullName (evt.GetType().FullName)

    let untypedGetKey f (evt : obj) metadata = 
        match evt with
        | :? 'TEvent as evt ->
            f evt metadata
        | _ -> failwith <| sprintf "Expecting type: %s but got type: %s" typeof<'TEvent>.FullName (evt.GetType().FullName)

    let handler (getKey : GetEventKey<'TMetadata, 'TEvent, 'TKey>) (f : HandlerFunction<'TState, 'TMetadata, 'TEvent>) (b : EventFold<'TState, 'TMetadata, 'TKey>) =
        b.AddHandler <| SingleEvent (typeof<'TEvent>, untypedGetKey getKey, untypedHandler f)