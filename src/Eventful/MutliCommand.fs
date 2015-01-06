namespace Eventful

open FSharpx

module MutliCommand = 
    type MultiCommandLanguage<'N,'TCommandContext,'TResult> =
    | RunCommand of obj * 'TCommandContext * ('TResult -> Async<'N>)
    | NotYetDone of (unit -> 'N)
    and 
        FreeMutliCommand<'F,'R,'TCommandContext,'TResult> = 
        | FreeMutliCommand of MultiCommandLanguage<FreeMutliCommand<'F,'R,'TCommandContext,'TResult>,'TCommandContext,'TResult>
        | Pure of 'R
    and
        MutliCommandProgram<'A,'TCommandContext,'TResult> = FreeMutliCommand<obj,'A,'TCommandContext,'TResult>

    let fmap f command = 
        match command with
        | NotYetDone (delay) ->
            NotYetDone (fun () -> f (delay()))
        | RunCommand (cmd, cmdContext, next) -> 
            RunCommand (cmd, cmdContext, (fun x -> next x |> Async.map f))

    let empty = Pure ()

    let liftF command = FreeMutliCommand (fmap Pure command)

    let runCommand cmd cmdContext = 
        RunCommand (cmd, cmdContext, id) |> liftF

    let rec bind f v =
        match v with
        | FreeMutliCommand x -> FreeMutliCommand (fmap (bind f) x)
        | Pure r -> f r

    // Return the final value wrapped in the Free type.
    let result value = Pure value

    // The whileLoop operator.
    // This is boilerplate in terms of "result" and "bind".
    let rec whileLoop pred body =
        if pred() then body |> bind (fun _ -> whileLoop pred body)
        else result ()

    // The delay operator.
    let delay (func : unit -> FreeMutliCommand<'a,'b,'TCommandContext,'TResult>) : FreeMutliCommand<'a,'b,'TCommandContext,'TResult> = 
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
        member x.Return(r:'R) : FreeMutliCommand<'F,'R,'TCommandContext,'TResult> = Pure r
        member x.ReturnFrom(r:FreeMutliCommand<'F,'R,'TCommandContext,'TResult>) : FreeMutliCommand<'F,'R,'TCommandContext,'TResult> = r
        member x.Bind (inp : FreeMutliCommand<'F,'R,'TCommandContext,'TResult>, body : ('R -> FreeMutliCommand<'F,'U,'TCommandContext,'TResult>)) : FreeMutliCommand<'F,'U,'TCommandContext,'TResult>  = bind body inp
        member x.Combine(expr1, expr2) = combine expr1 expr2
        member x.For(a, f) = forLoop a f 
        member x.While(func, body) = whileLoop func body
        member x.Delay(func) = delay func

    let multiCommand = new MultiCommandBuilder()
