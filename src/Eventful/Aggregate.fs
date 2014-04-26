namespace Eventful
    open System

    type IFunctionInvoker<'TResult> =
        abstract CmdType : Type
        abstract StateType : Type
        abstract Invoke : obj -> obj -> 'TResult

    type IState =
        abstract FoldState : obj -> obj -> obj
        abstract Zero : obj
        abstract StateType : Type

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

        member m.AddStateGenerator<'TState> (stateGen : IState) =
            new Aggregate<'TResult>(cmdHandlers, stateGen :: stateGenerators) 

        member m.GetCommandHandlers<'TCmd> (cmd : 'TCmd) =
            cmdHandlers
            |> List.filter (fun x -> x.CmdType = typeof<'TCmd>) 
            |> function
            | [handler] -> Some handler
            | [] -> None
            | _ -> failwith <| sprintf "More than one handler for cmd: %A in aggregate %A" typeof<'TCmd> (m.GetType())