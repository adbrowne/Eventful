namespace Eventful

open System

type IStateBuilder =
    inherit IComparable
    abstract member Fold : obj -> obj -> obj
    abstract member Zero : obj
    abstract member Name : string

type StateBuilder<'TState> = {
    zero : 'TState
    fold : 'TState -> obj -> 'TState
    name : string
}
with static member ToInterface<'TState> (sb : StateBuilder<'TState>) = {
        new IStateBuilder with 
             member this.Fold (state : obj) (evt : obj) = 
                match state with
                | :? 'TState as s ->    
                    let result =  sb.fold s evt
                    result :> obj
                | _ -> state
             member this.Zero = sb.zero :> obj
             member this.Name = sb.name

//        override x.Equals obj =
//            obj.GetType().FullName = x.GetType().FullName
//
//        override x.GetHashCode () = x.GetType().FullName.GetHashCode()
//
        interface IComparable with
            member this.CompareTo(obj) =
                match obj with
                | :? IStateBuilder as sc -> compare sb.name sc.Name
                | _ -> -1
    }


type cmdHandler = obj -> (string * IStateBuilder * (obj -> Choice<seq<obj>, seq<string>>))

type EventProcessingConfiguration = {
    CommandHandlers : Map<string, (Type * cmdHandler)>
    StateBuilders: Set<IStateBuilder>
    EventHandlers : Map<string, (Type *  seq<(obj -> seq<(string *  IStateBuilder * (obj -> seq<obj>))>)>)>
}
with static member Empty = { CommandHandlers = Map.empty; StateBuilders = Set.empty; EventHandlers = Map.empty } 

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventProcessingConfiguration =
    let addCommand<'TCmd, 'TState> (toId : 'TCmd -> string) (stateBuilder : IStateBuilder) (handler : 'TCmd -> 'TState -> Choice<seq<obj>, seq<string>>) (config : EventProcessingConfiguration) = 
        let cmdType = typeof<'TCmd>.FullName
        let outerHandler (cmdObj : obj) =
            let realHandler (cmd : 'TCmd) =
                let stream = toId cmd
                let realRealHandler = 
                    let blah = handler cmd
                    fun (state : obj) ->
                        blah (state :?> 'TState)
                (stream, stateBuilder, realRealHandler)
            match cmdObj with
            | :? 'TCmd as cmd -> realHandler cmd
            | _ -> failwith <| sprintf "Unexpected command type: %A" (cmdObj.GetType())
        config
        |> (fun config -> { config with CommandHandlers = config.CommandHandlers |> Map.add cmdType (typeof<'TCmd>, outerHandler) })
    let addEvent<'TEvt, 'TState> (toId: 'TEvt -> string seq) (stateBuilder : IStateBuilder) (handler : 'TEvt -> 'TState -> seq<obj>) (config : EventProcessingConfiguration) =
        let evtType = typeof<'TEvt>.Name
        let outerHandler (evtObj : obj) : seq<(string * IStateBuilder * (obj -> seq<obj>))> =
            let realHandler (evt : 'TEvt) : seq<(string * IStateBuilder * (obj -> seq<obj>))> =
                toId evt
                |> Seq.map (fun stream ->
                    let realRealHandler = 
                        let blah = handler evt
                        fun (state : obj) ->
                            blah (state :?> 'TState)
                    (stream, stateBuilder, realRealHandler))
            match evtObj with
            | :? 'TEvt as evt -> realHandler evt
            | _ -> failwith <| sprintf "Unexpected event type: %A" (evtObj.GetType())
        match config.EventHandlers |> Map.tryFind evtType with
        | Some (_, existing) -> { config with EventHandlers = config.EventHandlers |> Map.add evtType (typeof<'TEvt>, existing |> Seq.append (Seq.singleton outerHandler)) }
        | None ->  { config with EventHandlers = config.EventHandlers |> Map.add evtType (typeof<'TEvt>, Seq.singleton outerHandler) }