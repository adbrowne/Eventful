namespace Eventful.EventStore

open Eventful
open Eventful.MultiCommand

open FSharpx

module MultiCommandInterpreter = 
    let log = createLogger "Eventful.EventStore.MultiCommandInterpreter"

    let interpret 
        (prog : MultiCommandProgram<'TResult,'TCommandContext,CommandResult<'TBaseType,'TMetadata>>)
        (runCommand : obj -> 'TCommandContext -> (Async<CommandResult<'TBaseType,'TMetadata>>))
        : Async<'TResult> = 

        let rec loop program : Async<'TResult> =
            match program with
            | FreeMultiCommand (RunCommand asyncBlock) -> async {
                    let! (cmd, cmdCtx, next) = asyncBlock
                    let! result = runCommand cmd cmdCtx
                    return! loop <| next result
                }
            | FreeMultiCommand (RunAsync (asyncBlock, next)) -> async {
                    let! result = asyncBlock
                    return! loop <| next result
                }
            | FreeMultiCommand (NotYetDone g) ->
                async {
                    let next = g ()
                    return! loop next 
                }
            | Exception exn ->
                raise exn
            | Pure (result : 'TResult) ->
                result |> Async.returnM

        loop prog 