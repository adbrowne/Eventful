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

type StateRunner<'TMetadata, 'TState, 'TEvent> = 'TEvent -> 'TMetadata -> 'TState -> 'TState

type UnitStateBuilder<'TState, 'TMetadata, 'TKey when 'TKey : equality>
    (
        name: string, 
        initialState : 'TState, 
        handlers : UnitStateBuilderHandler<'TState, 'TMetadata, 'TKey> list
    ) = 

    static member Empty name initialState = new UnitStateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, List.empty)

    member x.InitialState = initialState

    member x.AddHandler<'T> (f:UnitStateBuilderHandler<'TState, 'TMetadata, 'TKey>) =
        new UnitStateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, f::handlers)
        
    member x.GetRunners<'TEvent> () : (('TEvent -> 'TMetadata -> 'TKey) * StateRunner<'TMetadata, 'TState, 'TEvent>) seq = 
        seq {
            for handler in handlers do
               match handler with
               | AllEvents (getKey, handlerFunction) ->
                    let getKey _ metadata = getKey metadata
                    let stateRunner (evt : 'TEvent) metadata state = 
                        handlerFunction (state, evt, metadata)
                    yield (getKey, stateRunner)
               | SingleEvent (eventType, getKey, handlerFunction) ->
                    if eventType = typeof<'TEvent> then
                        let getKey evt metadata = getKey evt metadata
                        let stateRunner (evt : 'TEvent) metadata state = 
                            handlerFunction (state, evt, metadata)
                        yield (getKey, stateRunner)
        }

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

    let run (key : 'TKey) (evt : 'TEvent) (metadata : 'TMetadata) (builder: UnitStateBuilder<'TState, 'TMetadata, 'TKey> , currentState : 'TState) =
        let keyHandlers = 
            builder.GetRunners<'TEvent>()
            |> Seq.map (fun (getKey, handler) -> (getKey evt metadata, handler))
            |> Seq.filter (fun (k, _) -> k = key)
            |> Seq.map snd

        let acc state (handler : StateRunner<'TMetadata, 'TState, 'TEvent>) =
            handler evt metadata state

        let state' = keyHandlers |> Seq.fold acc currentState
        (builder, state')

    let getKeys (evt : 'TEvent) (metadata : 'TMetadata) (builder: UnitStateBuilder<'TState, 'TMetadata, 'TKey>) =
        builder.GetRunners<'TEvent>()
        |> Seq.map (fun (getKey, _) -> (getKey evt metadata))
        |> Seq.distinct