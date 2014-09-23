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

type StateBuilder<'TState>(initialState : 'TState, handlers : List<('TState -> obj -> 'TState)>, types : List<Type>) = 
    member x.InitialState = initialState
    member x.Types = types
    member x.AddHandler<'T> (f:accumulator<'TState, 'T>) =
        let func (state : 'TState) (message : obj) =
            match message with
            | :? 'T as msg -> f state msg
            | _ -> state

        let msgType = typeof<'T>
        new StateBuilder<'TState>(initialState, func::handlers, (typeof<'T>)::types)
        
    member x.Run (state: 'TState) (item: obj) =
        handlers
        |> List.fold (fun s h -> h s item) state

    static member Combine<'TState1, 'TState2, 'TState> (builder1 : StateBuilder<'TState1>) (builder2 : StateBuilder<'TState2>) combiner extractor =
       let initialState = combiner builder1.InitialState builder2.InitialState 
       let handler (state : 'TState) (message : obj) = 
            let (state1, state2) = extractor state
            let state1' = builder1.Run state1 message
            let state2' = builder2.Run state2 message
            combiner state1' state2'

       let types = Seq.append builder1.Types builder2.Types |> System.Linq.Enumerable.Distinct |> List.ofSeq
       new StateBuilder<'TState>(initialState, [handler], types)
    static member Empty initialState = new StateBuilder<'TState>(initialState, List.empty, List.empty)

type ChildStateBuilder<'TState,'TChildId>(idMapper:IdMapper<'TChildId>, stateBuilder : StateBuilder<'TState>) =
    member x.InitialState = stateBuilder.InitialState
    member x.GetId = idMapper.Run
    member x.RunState = stateBuilder.Run
    member x.Types = stateBuilder.Types
    static member Build (idMapper:IdMapper<'TChildId>) (stateBuilder:StateBuilder<'TState>) =
        new ChildStateBuilder<_,_>(idMapper, stateBuilder)
    static member BuildWithMagicMapper<'TChildId> (stateBuilder:StateBuilder<'TState>) =
        for t in stateBuilder.Types do
            match MagicMapper.magicPropertyGetter<'TChildId> t with
            | Some _ -> ()
            | None -> failwith <| sprintf "Cannot get id of type %A from type %A" typeof<'TChildId> t
        let handler obj =
            let t = obj.GetType()
            match MagicMapper.magicPropertyGetter t with 
            | Some getter -> Some (getter obj)
            | None -> None

        let idMapper = new IdMapper<'TChildId>([handler], stateBuilder.Types)
        new ChildStateBuilder<_,_>(idMapper, stateBuilder)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module StateBuilder =
    let Counter<'T> = 
        let s = StateBuilder.Empty 0
        s.AddHandler (fun c (x : 'T) -> c + 1)

    let SetBuilder<'T, 'TAddMessage, 'TRemoveMessage when 'T : comparison> (addItem : 'TAddMessage -> 'T) (removeItem: 'TRemoveMessage -> 'T)  =
        let s = StateBuilder.Empty (Set.empty : Set<'T>)
        let s = s.AddHandler((fun (x : Set<'T>) (add : 'TAddMessage) -> x |> Set.add (addItem add)))
        let s = s.AddHandler((fun (x : Set<'T>) (remove : 'TRemoveMessage) -> x |> Set.remove (removeItem remove)))
        s

    let lastValue<'TState, 'TEvent> (f : 'TEvent -> 'TState) (initialState : 'TState) = 
        let s = StateBuilder.Empty initialState
        s.AddHandler (fun _ m -> f m)

    let mapMessages<'TState,'TInputEvent, 'TOutputEvent> (f : 'TInputEvent -> 'TOutputEvent) (sb : StateBuilder<'TState>)=
        let sb' = StateBuilder.Empty sb.InitialState
        let mappingAccumulator s i =
            let outputValue = f i
            sb.Run s (outputValue :> obj)
        sb'.AddHandler<'TInputEvent> mappingAccumulator

    let addHandler<'T, 'TEvent> (f : accumulator<'T,'TEvent>) (b : StateBuilder<'T>) =
        b.AddHandler f

    let mapHandler<'TMsg,'TKey,'TState when 'TKey : comparison> 
        (getKey : 'TMsg -> 'TKey) 
        (accumulator : accumulator<'TState,'TMsg>) 
        (initialState : 'TState) 
        : accumulator<Map<'TKey,'TState>,'TMsg> =
        let acc (state : Map<'TKey,'TState>) msg =
            let key = getKey msg
            let childState = 
                if state |> Map.containsKey key then
                    state |> Map.find key
                else
                    initialState

            let childState' = accumulator childState msg
            state |> Map.add key childState'
        acc

    let map2 combine extract (stateBuilder1:StateBuilder<_>) (stateBuilder2:StateBuilder<_>) =
        let initialState = combine stateBuilder1.InitialState stateBuilder2.InitialState
        let types = stateBuilder1.Types |> Seq.append stateBuilder2.Types |> Seq.distinct |> List.ofSeq
        let handler state event =
            let (s1,s2) = extract state
            let s1' = stateBuilder1.Run s1 event
            let s2' = stateBuilder2.Run s2 event
            combine s1' s2'

        new StateBuilder<_>(initialState, [handler], types)

    // this is a pattern through these but I don't have a name
    let weirdApply2 f g = (fun a b -> f (g a b))
    let weirdApply3 f g = (fun a b c -> f (g a b c))
    let weirdApply4 f g = (fun a b c d -> f (g a b c d))
    let weirdApply5 f g = (fun a b c d e -> f (g a b c d e))
    let weirdApply6 f_ g_ = (fun a b c d e f -> f_ (g_ a b c d e f))
    let weirdApply7 f_ g_ = (fun a b c d e f g -> f_ (g_ a b c d e f g))

    let map3 combine extract stateBuilder1 =
        let combiner a = applyTuple2 (combine a)
        let extractor = extract >> tupleFst3
        weirdApply2 (map2 combiner extractor stateBuilder1) (map2 tuple2 id)

    let map4 combine extract stateBuilder1 =
        let combiner a = applyTuple3 (combine a)
        let extractor = extract >> tupleFst4
        weirdApply3 (map2 combiner extractor stateBuilder1) (map3 tuple3 id)

    let map5 combine extract stateBuilder1 =
        let combiner a = applyTuple4 (combine a)
        let extractor = extract >> tupleFst5
        weirdApply4 (map2 combiner extractor stateBuilder1) (map4 tuple4 id)

    let map6 combine extract stateBuilder1 =
        let combiner a = applyTuple5 (combine a)
        let extractor = extract >> tupleFst6
        weirdApply5 (map2 combiner extractor stateBuilder1) (map5 tuple5 id)

    let map7 combine extract stateBuilder1 =
        let combiner a = applyTuple6 (combine a)
        let extractor = extract >> tupleFst7
        weirdApply6 (map2 combiner extractor stateBuilder1) (map6 tuple6 id)

    let runState<'TState> (stateBuilder : StateBuilder<'TState>) (items : obj list) =
        items
        |> List.fold stateBuilder.Run stateBuilder.InitialState

    let toMap<'TId,'TState when 'TId : comparison> (childStateBuilder:ChildStateBuilder<'TState,'TId>) : StateBuilder<Map<'TId,'TState>> =
        let handler state e =
            let eventType = e.GetType()
            if (childStateBuilder.Types |> List.exists(fun i -> i = eventType)) then
                let id = childStateBuilder.GetId e
                let subState = 
                    match state |> Map.tryFind id with
                    | Some subState -> subState
                    | None -> childStateBuilder.InitialState

                let newSubState = childStateBuilder.RunState subState e
                state |> Map.add id newSubState
            else
                state
                
        new StateBuilder<_>(Map.empty, [handler], childStateBuilder.Types)

    let NoState = StateBuilder.Empty ()

    let toStreamProgram streamName (stateBuilder:StateBuilder<'TState>) = eventStream {
        let rec loop eventsConsumed state = eventStream {
            let! token = readFromStream streamName eventsConsumed
            match token with
            | Some token -> 
                let! (value, metadata) = readValue token
                let currentState = state |> Option.getOrElse stateBuilder.InitialState
                let state' = stateBuilder.Run currentState value
                return! loop (eventsConsumed + 1) (Some state')
            | None -> 
                return (eventsConsumed, state) }
            
        return! loop 0 None
    }

type IUntypedStateBuilder<'TMetadata> =
    abstract Name : string
    abstract Apply : obj * obj * 'TMetadata -> obj
    abstract InitialState : obj
    abstract MessageTypes : List<Type>

type INamedStateBuilder<'TState, 'TMetadata> =
    inherit IUntypedStateBuilder<'TMetadata>
    abstract Name : string
    abstract Apply : 'TState * obj * 'TMetadata -> 'TState
    abstract InitialState : 'TState
    abstract MessageTypes : List<Type>

type IKeyedStateBuilder<'TState, 'TMetadata, 'TKey> =
    abstract Name : string
    abstract Apply : 'TState * obj * 'TMetadata -> 'TState
    abstract InitialState : 'TState
    abstract MessageTypes : List<Type>
    abstract GetKey : obj * 'TMetadata -> 'TKey

type NamedStateBuilder<'TState,'TMetadata>(name : string, builder: StateBuilder<'TState>) =
    member x.Name = name
    member x.Builder = builder
    member x.MessageTypes = builder.Types
    member x.Apply (state : obj) objWithMetadata = 
        let (message, metadata) = objWithMetadata
        let typedState = state :?> 'TState
        builder.Run typedState message

    interface IUntypedStateBuilder<'TMetadata> with
        member x.Name = name
        member x.MessageTypes = builder.Types
        member x.Apply (state, msg, metadata) = (x.Apply (state, msg, metadata)) :> obj
        member x.InitialState = builder.InitialState :> obj
    interface INamedStateBuilder<'TState, 'TMetadata> with
        member x.Name = name
        member x.Apply (state, message, metadata) = 
            builder.Run state message
        member x.MessageTypes = builder.Types
        member x.InitialState = builder.InitialState

type KeyedStateBuilder<'TState, 'TMetadata, 'TKey> 
    (
        name : string,
        fKey : 'TMetadata -> 'TKey,
        builder : StateBuilder<'TState>
    ) = 
    interface IUntypedStateBuilder<'TMetadata> with
        member x.Name = name
        member x.InitialState = builder.InitialState :> obj
        member x.MessageTypes = builder.Types
        member x.Apply (state, message, metadata)  = 
            let typedState = state :?> 'TState
            builder.Run typedState message :> obj
    interface INamedStateBuilder<'TState, 'TMetadata> with
        member x.Name = name
        member x.Apply (state, message, metadata) = 
            builder.Run state message
        member x.MessageTypes = builder.Types
        member x.InitialState = builder.InitialState
    interface IKeyedStateBuilder<'TState, 'TMetadata, 'TKey> with
        member x.Name = name
        member x.Apply (state, message, metadata) = 
            builder.Run state message
        member x.MessageTypes = builder.Types
        member x.GetKey (evt,metadata) = fKey metadata
        member x.InitialState = builder.InitialState

module NamedStateBuilder =
    let withName (name : string) (builder : StateBuilder<'TState>) : NamedStateBuilder<'TState,'TMetadata> =
        new NamedStateBuilder<_,_>(name, builder)

    let withKey (name : string) (fKey : 'TMetadata -> 'TKey) (builder : StateBuilder<'TState>) : KeyedStateBuilder<'TState, 'TMetadata, 'TKey> =
        new KeyedStateBuilder<_,_,_>(name, fKey, builder)

    let nullStateBuilder<'TMetadata> : NamedStateBuilder<unit, 'TMetadata> = 
        StateBuilder.Empty () |> withName "$Empty"

type CombinedStateBuilderState = Map<string,obj>

type CombinedStateBuilder<'TMetadata> internal (children : Map<string, IUntypedStateBuilder<'TMetadata>>) =
    member x.AddChild (c : IUntypedStateBuilder<'TMetadata>) =
        match children |> Map.tryFind c.Name with
        | Some n when n = c ->
            x // we are adding the same item twice - this is fine just return ourselves
        | Some n ->
            failwith "Cannot combine different NamedStateBuilders with the same name"
        | None ->
            new CombinedStateBuilder<'TMetadata>(children |> Map.add c.Name c)

    member x.Run inputStates e =
        let runChildState (item: obj, metadata : 'TMetadata) (s:CombinedStateBuilderState) (childKey : string) (childBuilder : IUntypedStateBuilder<'TMetadata>) =
            let currentState = inputStates |> Map.tryFind childKey |> Option.getOrElse childBuilder.InitialState
            let newState = childBuilder.Apply(currentState,item,metadata)
            s |> Map.add childKey newState
            
        children |> Map.fold (runChildState e) Map.empty

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CombinedStateBuilder =
    let empty<'TMetadata> = new CombinedStateBuilder<'TMetadata>(Map.empty)

    let add child (s:CombinedStateBuilder<'TMetadata>) =
        s.AddChild(child)

    let run inputStates item (s:CombinedStateBuilder<'TMetadata>) =
        s.Run inputStates item

    let getValueByName (name : string) (state : CombinedStateBuilderState) =
        state |> Map.find name
    let getValue (builder:NamedStateBuilder<'TState,'TMetadata>) (state : CombinedStateBuilderState) =
        (getValueByName builder.Name state) :?> 'TState

    let toStreamProgram streamName (stateBuilder:CombinedStateBuilder<'TMetadata>) = eventStream {
        let rec loop eventsConsumed state = eventStream {
            let! token = readFromStream streamName eventsConsumed
            match token with
            | Some token -> 
                let! (value, metadata) = readValue token
                let currentState = state |> Option.getOrElse Map.empty
                let state' = stateBuilder.Run currentState (value, metadata)
                return! loop (eventsConsumed + 1) (Some state')
            | None -> 
                return (eventsConsumed, state) }
            
        return! loop 0 None
    }