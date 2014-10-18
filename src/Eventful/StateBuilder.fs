namespace Eventful

open System
open FSharpx
open Eventful

type StateRunner<'TMetadata, 'TState, 'TEvent> = 'TEvent -> 'TMetadata -> 'TState -> 'TState

type HandlerFunction<'TState, 'TMetadata, 'TEvent> = 'TState * 'TEvent * 'TMetadata -> 'TState
type GetAllEventsKey<'TMetadata, 'TKey> = 'TMetadata -> 'TKey
type GetEventKey<'TMetadata, 'TEvent, 'TKey> = 'TEvent -> 'TMetadata -> 'TKey

type IStateBlockBuilder<'TMetadata, 'TKey> = 
    abstract Type : Type
    abstract Name : string
    abstract InitialState : obj
    abstract GetRunners<'TEvent> : unit -> (GetEventKey<'TMetadata, 'TEvent, 'TKey> * StateRunner<'TMetadata, Map<string,obj>, 'TEvent>) seq

type IStateBuilder<'TState, 'TMetadata, 'TKey> = 
    abstract GetBlockBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list
    abstract GetState : Map<string, obj> -> 'TState

type StateBuilderHandler<'TState, 'TMetadata, 'TKey> = 
    | AllEvents of GetAllEventsKey<'TMetadata, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>
    | SingleEvent of Type * GetEventKey<'TMetadata, obj, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>

type StateBuilder<'TState, 'TMetadata, 'TKey when 'TKey : equality>
    (
        name: string, 
        initialState : 'TState, 
        handlers : StateBuilderHandler<'TState, 'TMetadata, 'TKey> list
    ) = 

    let getStateFromMap (stateMap : Map<string,obj>) =
       stateMap 
       |> Map.tryFind name 
       |> Option.map (fun s -> s :?> 'TState)
       |> Option.getOrElse initialState 

    static member Empty name initialState = new StateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, List.empty)

    member x.InitialState = initialState

    member x.AddHandler<'T> (h:StateBuilderHandler<'TState, 'TMetadata, 'TKey>) =
        new StateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, h::handlers)

    member x.GetRunners<'TEvent> () : (GetEventKey<'TMetadata, 'TEvent, 'TKey> * StateRunner<'TMetadata, 'TState, 'TEvent>) seq = 
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

    interface IStateBlockBuilder<'TMetadata, 'TKey> with
        member x.Name = name
        member x.Type = typeof<'TState>
        member x.InitialState = initialState :> obj
        member x.GetRunners<'TEvent> () =
            x.GetRunners<'TEvent> ()
            |> Seq.map 
                (fun (getKey, handler) ->
                    let mapHandler evt metadata (stateMap : Map<string,obj>) =
                        let state = getStateFromMap stateMap 
                        let state' = handler evt metadata state
                        stateMap |> Map.add name (state' :> obj)

                    (getKey, mapHandler)
                )

    interface IStateBuilder<'TState, 'TMetadata, 'TKey> with
        member x.GetBlockBuilders = [x :> IStateBlockBuilder<'TMetadata, 'TKey>]
        member x.GetState stateMap = getStateFromMap stateMap

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module StateBuilder =
    let nullStateBuilder<'TMetadata, 'TKey when 'TKey : equality> =
        StateBuilder<unit, 'TMetadata, 'TKey>.Empty "$Empty" ()

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

    let handler (getKey : GetEventKey<'TMetadata, 'TEvent, 'TKey>) (f : HandlerFunction<'TState, 'TMetadata, 'TEvent>) (b : StateBuilder<'TState, 'TMetadata, 'TKey>) =
        b.AddHandler <| SingleEvent (typeof<'TEvent>, untypedGetKey getKey, untypedHandler f)

    let allEventsHandler getKey f (b : StateBuilder<'TState, 'TMetadata, 'TKey>) =
        b.AddHandler <| AllEvents (getKey, untypedHandler f)

    let run (key : 'TKey) (evt : 'TEvent) (metadata : 'TMetadata) (builder: StateBuilder<'TState, 'TMetadata, 'TKey> , currentState : 'TState) =
        let keyHandlers = 
            builder.GetRunners<'TEvent>()
            |> Seq.map (fun (getKey, handler) -> (getKey evt metadata, handler))
            |> Seq.filter (fun (k, _) -> k = key)
            |> Seq.map snd

        let acc state (handler : StateRunner<'TMetadata, 'TState, 'TEvent>) =
            handler evt metadata state

        let state' = keyHandlers |> Seq.fold acc currentState
        (builder, state')

    let getKeys (evt : 'TEvent) (metadata : 'TMetadata) (builder: StateBuilder<'TState, 'TMetadata, 'TKey>) =
        builder.GetRunners<'TEvent>()
        |> Seq.map (fun (getKey, _) -> (getKey evt metadata))
        |> Seq.distinct

type AggregateStateBuilder<'TState, 'TMetadata, 'TKey when 'TKey : equality>
    (
        unitBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list,
        extract : Map<string, obj> -> 'TState
    ) = 

    static member Empty name initialState = new StateBuilder<'TState, 'TMetadata, 'TKey>(name, initialState, List.empty)

    member x.InitialState = 
        let acc s (b : IStateBlockBuilder<'TMetadata, 'TKey>) =
            s |> Map.add b.Name b.InitialState

        unitBuilders 
        |> List.fold acc Map.empty
        |> extract

    interface IStateBuilder<'TState, 'TMetadata, 'TKey> with
        member x.GetBlockBuilders = unitBuilders
        member x.GetState unitStates = extract unitStates

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module AggregateStateBuilder =
    let combine f (b1 : IStateBuilder<'TState1, 'TMetadata, 'TKey>) (b2 : IStateBuilder<'TState2, 'TMetadata, 'TKey>) : IStateBuilder<'TStateCombined, 'TMetadata, 'TKey> =
        let combinedUnitBuilders = 
            Seq.append b1.GetBlockBuilders b2.GetBlockBuilders 
            |> Seq.distinct
            |> List.ofSeq

        let extract unitStates = 
            f (b1.GetState unitStates) (b2.GetState unitStates)

        new AggregateStateBuilder<'TStateCombined, 'TMetadata, 'TKey>(combinedUnitBuilders, extract) :> IStateBuilder<'TStateCombined, 'TMetadata, 'TKey>

    let combineHandlers (h1 : IStateBlockBuilder<'TMetadata, 'TId> list) (h2 : IStateBlockBuilder<'TMetadata, 'TId> list) =
        List.append h1 h2 
        |> Seq.distinct
        |> List.ofSeq

    let ofStateBuilderList (builders : IStateBlockBuilder<'TMetadata, 'TKey> list) =
        new AggregateStateBuilder<Map<string,obj>,'TMetadata, 'TKey>(builders, id)

    let run<'TMetadata, 'TKey, 'TEvent when 'TKey : equality> (unitBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list) key evt metadata currentUnitStates =
        let runBuilder unitStates (builder : IStateBlockBuilder<'TMetadata, 'TKey>) = 
            let keyHandlers = 
                builder.GetRunners<'TEvent>()
                |> Seq.map (fun (getKey, handler) -> (getKey evt metadata, handler))
                |> Seq.filter (fun (k, _) -> k = key)
                |> Seq.map snd

            let acc state (handler : StateRunner<'TMetadata, 'TState, 'TEvent>) =
                handler evt metadata state

            let state' = keyHandlers |> Seq.fold acc unitStates
            state'

        unitBuilders |> List.fold runBuilder currentUnitStates

    let genericRunMethod = 
        let moduleInfo = 
          System.Reflection.Assembly.GetExecutingAssembly().GetTypes()
          |> Seq.find (fun t -> t.FullName = "Eventful.AggregateStateBuilderModule")
        let name = "run"
        moduleInfo.GetMethod(name)

    let dynamicRun (unitBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list) key evt metadata currentUnitStates =
        let specializedMethod = genericRunMethod.MakeGenericMethod(typeof<'TMetadata>, typeof<'TKey>, evt.GetType())
        specializedMethod.Invoke(null, [| unitBuilders; key; evt; metadata; currentUnitStates |]) :?> Map<string, obj>

    let map (f : 'T1 -> 'T2) (stateBuilder: IStateBuilder<'T1, 'TMetadata, 'TKey>) =
        let extract unitStates = stateBuilder.GetState unitStates |> f
        new AggregateStateBuilder<'T2, 'TMetadata, 'TKey>(stateBuilder.GetBlockBuilders, extract) :> IStateBuilder<'T2, 'TMetadata, 'TKey>

    let toStreamProgram streamName key (stateBuilder:IStateBuilder<'TState, 'TMetadata, 'TKey>) = EventStream.eventStream {
        let rec loop eventsConsumed currentState = EventStream.eventStream {
            let! token = EventStream.readFromStream streamName eventsConsumed
            match token with
            | Some token -> 
                let! (value, metadata : 'TMetadata) = EventStream.readValue token
                let state' = dynamicRun stateBuilder.GetBlockBuilders key value metadata currentState
                return! loop (eventsConsumed + 1) state'
            | None -> 
                return (eventsConsumed, currentState) }
            
        return! loop 0 Map.empty
    }

    let tuple2 b1 b2 =
        combine FSharpx.Prelude.tuple2 b1 b2

    let tuple3 b1 b2 b3 =
        (tuple2 b2 b3)
        |> combine FSharpx.Prelude.tuple2 b1
        |> map (fun (a,(b,c)) -> (a,b,c))

    let tuple4 b1 b2 b3 b4 =
        (tuple3 b2 b3 b4)
        |> combine FSharpx.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d)) -> (a,b,c,d))

    let tuple5 b1 b2 b3 b4 b5 =
        (tuple4 b2 b3 b4 b5)
        |> combine FSharpx.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e)) -> (a,b,c,d,e))

    let tuple6 b1 b2 b3 b4 b5 b6 =
        (tuple5 b2 b3 b4 b5 b6)
        |> combine FSharpx.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e,f)) -> (a,b,c,d,e,f))

    let tuple7 b1 b2 b3 b4 b5 b6 b7 =
        (tuple6 b2 b3 b4 b5 b6 b7)
        |> combine FSharpx.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e,f,g)) -> (a,b,c,d,e,f,g))

    let tuple8 b1 b2 b3 b4 b5 b6 b7 b8 =
        (tuple7 b2 b3 b4 b5 b6 b7 b8)
        |> combine FSharpx.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e,f,g,h)) -> (a,b,c,d,e,f,g,h))