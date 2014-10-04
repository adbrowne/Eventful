namespace Eventful

open System

type IAggregateStateBuilder<'TState, 'TMetadata, 'TKey> = 
    abstract Name : string
    abstract Apply : 'TState * obj * 'TMetadata -> 'TState
    abstract InitialState : 'TState
    abstract MessageTypes : List<Type>
    abstract GetKey : obj * 'TMetadata -> 'TKey

type HandlerFunction<'TState, 'TMetadata, 'TEvent> = 'TState * 'TEvent * 'TMetadata -> 'TState
type GetAllEventsKey<'TMetadata, 'TKey> = 'TMetadata -> 'TKey
type GetEventKey<'TMetadata, 'TEvent, 'TKey> = 'TEvent -> 'TMetadata -> 'TKey

type UnitStateBuilderHandler<'TState, 'TMetadata, 'TKey> = 
    | AllEvents of GetAllEventsKey<'TMetadata, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>
    | SingleEvent of Type * GetEventKey<'TMetadata, obj, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>

type UnitStateBuilder<'TState, 'TMetadata, 'TKey>(name: string, initialState : 'TState, handlers : UnitStateBuilderHandler<'TState, 'TMetadata, 'TKey> list) = 
    static member Empty name initialState = new UnitStateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, List.empty)
    member x.InitialState = initialState
    member x.AddHandler<'T> (f:UnitStateBuilderHandler<'TState, 'TMetadata, 'TKey>) =
        new UnitStateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, f::handlers)
        
    member x.Run (currentState: 'TState) (item: obj) (metadata: 'TMetadata) =
        let acc state handler =
            match handler with
            | AllEvents (getKey, handlerFunction) ->
                handlerFunction (state, item, metadata)
            | SingleEvent (eventType, getKey, handlerFunction) ->
                if item.GetType() = eventType then
                    handlerFunction (state, item, metadata)
                else
                    state
        handlers
        |> List.fold acc currentState

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module UnitStateBuilder =
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

    let handler (getKey : GetEventKey<'TMetadata, 'TEvent, 'TKey>) (f : HandlerFunction<'TState, 'TMetadata, 'TEvent>) (b : UnitStateBuilder<'TState, 'TMetadata, 'TKey>) =
        b.AddHandler <| SingleEvent (typeof<'TEvent>, untypedGetKey getKey, untypedHandler f)

    let allEventsHandler getKey f (b : UnitStateBuilder<'TState, 'TMetadata, 'TKey>) =
        b.AddHandler <| AllEvents (getKey, untypedHandler f)

    let run (evt : 'TEvent) (metadata : 'TMetadata) (builder: UnitStateBuilder<'TState, 'TMetadata, 'TKey> , currentState : 'TState) =
        let state' = builder.Run currentState (evt :> obj) metadata
        (builder, state')