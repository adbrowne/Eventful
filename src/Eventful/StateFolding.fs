namespace Eventful

open System

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

    let mapHandler<'TMsg,'TKey,'TState when 'TKey : comparison> (getKey : 'TMsg -> 'TKey) (accumulator : accumulator<'TState,'TMsg>) (initialState : 'TState) : accumulator<Map<'TKey,'TState>,'TMsg> =
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

    let map3<'a,'b,'c,'d> (combine : 'a -> 'b -> 'c -> 'd) (extract : 'd -> ('a * 'b * 'c)) (stateBuilder1:StateBuilder<'a>) =
        let combine2 a  b = (a, b)
        let extract2 = id
        let combine3 a (b,c) = combine a b c
        let extract3 d = 
            let (a,b,c) = extract d
            (a,(b,c))
        (fun stateBuilder2 stateBuilder3 ->
            let stateBuilderRest = map2 combine2 extract2 stateBuilder2 stateBuilder3
            map2 combine3 extract3 stateBuilder1 stateBuilderRest)

    let runState<'TState> (stateBuilder : StateBuilder<'TState>) (items : obj list) =
        items
        |> List.fold stateBuilder.Run stateBuilder.InitialState

    let toMap<'TId,'TState when 'TId : comparison> (childStateBuilder:ChildStateBuilder<'TState,'TId>) : StateBuilder<Map<'TId,'TState>> =
        let handler state e =
            let id = childStateBuilder.GetId e
            let subState = 
                match state |> Map.tryFind id with
                | Some subState -> subState
                | None -> childStateBuilder.InitialState

            let newSubState = childStateBuilder.RunState subState e
            state |> Map.add id newSubState
                
        new StateBuilder<_>(Map.empty, [handler], childStateBuilder.Types)