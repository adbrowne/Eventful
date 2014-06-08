namespace Eventful
    open System

    type IFunctionInvoker<'TResult> =
        abstract CmdType : Type
        abstract StateType : Type
        abstract Invoke : obj -> obj -> 'TResult

    type SimpleFunctionInvoker<'TResult> (cmdType, stateType, f : obj -> obj -> 'TResult) =
        interface IFunctionInvoker<'TResult> with
            member m.Invoke a b = f a b
            member m.CmdType = cmdType
            member m.StateType = stateType

    type IState =
        abstract FoldState : obj -> obj -> obj
        abstract Zero : obj
        abstract StateType : Type

    type IStateT<'TState> =
        abstract FoldState : 'TState -> obj -> 'TState
        abstract Zero : 'TState

    type StateGen<'TState> (fold : 'TState -> obj -> 'TState, zero : 'TState) =
        member m.FoldState = fold
        member m.Zero = zero
        interface IState with
            member m.FoldState a b = (fold (a :?> 'TState) b) :> obj
            member m.Zero = zero :> obj
            member m.StateType = typeof<'TState>

    type internal FunctionWrapper<'TState, 'TCmd, 'TResult> (f : 'TState -> 'TCmd -> 'TResult) =
        interface IFunctionInvoker<'TResult> with
            member m.CmdType = typeof<'TCmd>
            member m.StateType = typeof<'TState>
            member m.Invoke (state : obj) (cmd : obj) =
                f (state :?> 'TState) (cmd :?> 'TCmd)

    type Aggregate<'TResult> (cmdHandlers : IFunctionInvoker<'TResult> list, stateGenerators : IState list) =
        static let empty =
            new Aggregate<'TResult>(List.empty, List.empty)

        static member Empty : Aggregate<'TResult> = empty

        member m.AddCmdHandler<'TState, 'TCmd> (f : 'TState -> 'TCmd -> 'TResult) =
            let wrappedCmd = new FunctionWrapper<'TState, 'TCmd, 'TResult>(f) :> IFunctionInvoker<'TResult>
            new Aggregate<'TResult>(wrappedCmd :: cmdHandlers, stateGenerators)

        member m.AddUntypedCmdHandler (tState : Type) (tCmd: Type) (f : obj -> obj -> 'TResult) = 
            let wrappedCmd = new SimpleFunctionInvoker<'TResult>(tCmd, tState, f) :> IFunctionInvoker<'TResult>
            new Aggregate<'TResult>(wrappedCmd :: cmdHandlers, stateGenerators)

        member m.AddStateGenerator<'TState> (stateGen : IState) =
            new Aggregate<'TResult>(cmdHandlers, stateGen :: stateGenerators) 

        member m.GetCommandHandlers<'TCmd> (cmd : 'TCmd) =
            cmdHandlers
            |> List.filter (fun x -> x.CmdType = typeof<'TCmd>)
            |> function
            | [handler] -> Some (handler, m)
            | [] -> None
            | _ -> failwith <| sprintf "More than one handler for cmd: %A in aggregate %A" typeof<'TCmd> (m.GetType())

        member m.GetZeroState () =
            let stateGen = stateGenerators |> List.head 
            stateGen.Zero