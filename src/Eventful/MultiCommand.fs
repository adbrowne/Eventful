namespace Eventful

open FSharpx

module MultiCommand = 
    type MultiCommandLanguage<'N,'TCommandContext,'TResult> =
    | RunCommand of (Async<(obj * 'TCommandContext * ('TResult -> 'N))>)
    | RunAsync of Async<obj> * (obj -> 'N)
    | NotYetDone of (unit -> 'N)
    and 
        FreeMultiCommand<'F,'R,'TCommandContext,'TResult> = 
        | FreeMultiCommand of MultiCommandLanguage<FreeMultiCommand<'F,'R,'TCommandContext,'TResult>,'TCommandContext,'TResult>
        | Pure of 'R
    and
        MultiCommandProgram<'A,'TCommandContext,'TResult> = FreeMultiCommand<obj,'A,'TCommandContext,'TResult>

    let fmap f command = 
        match command with
        | NotYetDone (delay) ->
            NotYetDone (fun () -> f (delay()))
        | RunCommand asyncBlock -> 
            RunCommand (asyncBlock |> Async.map (fun (cmd, cmdCtx, next) -> (cmd, cmdCtx, next >> f)))
        | RunAsync (asyncBlock, next)-> 
            RunAsync (asyncBlock, next >> f)

    let empty = Pure ()

    let liftF command = FreeMultiCommand (fmap Pure command)

    let runCommand cmd cmdContext = 
        RunCommand ((cmd, cmdContext, id) |> Async.returnM) |> liftF

    let runCommandAsync (getCmd : Async<(obj * 'TCommandContext)>) = 
        RunCommand (getCmd |> Async.map (fun (cmd, ctx) -> (cmd, ctx, id))) |> liftF

    let runAsync (asyncBlock : Async<'TResult>) = 
        RunAsync (asyncBlock |> Async.map (fun x -> x :> obj), (fun x -> x :?> 'TResult)) |> liftF

    let rec bind f v =
        match v with
        | FreeMultiCommand x -> FreeMultiCommand (fmap (bind f) x)
        | Pure r -> f r

    // Return the final value wrapped in the Free type.
    let result value = Pure value

    // The whileLoop operator.
    // This is boilerplate in terms of "result" and "bind".
    let rec whileLoop pred body =
        if pred() then body |> bind (fun _ -> whileLoop pred body)
        else result ()

    // The delay operator.
    let delay (func : unit -> FreeMultiCommand<'a,'b,'TCommandContext,'TResult>) : FreeMultiCommand<'a,'b,'TCommandContext,'TResult> = 
        let notYetDone = NotYetDone (fun () -> ()) |> liftF
        bind func notYetDone

    // The sequential composition operator.
    // This is boilerplate in terms of "result" and "bind".
    let combine expr1 expr2 =
        expr1 |> bind (fun () -> expr2)

    // The forLoop operator.
    // This is boilerplate in terms of "catch", "result", and "bind".
    let forLoop (collection:seq<_>) func =
        let ie = collection.GetEnumerator()
        (whileLoop (fun () -> ie.MoveNext())
            (delay (fun () -> let value = ie.Current in func value)))

    type MultiCommandBuilder() =
        member x.Zero() = Pure ()
        member x.Return(r:'R) : FreeMultiCommand<'F,'R,'TCommandContext,'TResult> = Pure r
        member x.ReturnFrom(r:FreeMultiCommand<'F,'R,'TCommandContext,'TResult>) : FreeMultiCommand<'F,'R,'TCommandContext,'TResult> = r
        member x.Bind (inp : FreeMultiCommand<'F,'R,'TCommandContext,'TResult>, body : ('R -> FreeMultiCommand<'F,'U,'TCommandContext,'TResult>)) : FreeMultiCommand<'F,'U,'TCommandContext,'TResult>  = bind body inp
        member x.Combine(expr1, expr2) = combine expr1 expr2
        member x.For(a, f) = forLoop a f 
        member x.While(func, body) = whileLoop func body
        member x.Delay(func) = delay func

    let multiCommand = new MultiCommandBuilder()