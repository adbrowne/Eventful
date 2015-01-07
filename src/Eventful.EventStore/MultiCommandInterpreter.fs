namespace Eventful.EventStore

open Eventful
open Eventful.MultiCommand

module MultiCommandInterpreter = 
    let log = createLogger "Eventful.EventStore.MultiCommandInterpreter"

    let interpret 
        (prog : MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseType,'TMetadata>>)
        (runCommand : obj -> 'TCommandContext -> (Async<CommandResult<'TBaseType,'TMetadata>>))
        : Async<unit> = 

        let rec loop program : Async<unit> =
            match program with
            | FreeMultiCommand (RunCommand asyncBlock) -> async {
                    let! (cmd, cmdCtx, next) = asyncBlock
                    let! result = runCommand cmd cmdCtx
                    return! loop <| next result
                }
            | FreeMultiCommand (NotYetDone g) ->
                async {
                    let next = g ()
                    return! loop next 
                }
            | Pure result ->
                async { () }

        loop prog 