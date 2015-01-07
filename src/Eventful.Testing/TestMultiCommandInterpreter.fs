namespace Eventful.Testing

open Eventful
open Eventful.MultiCommand
open FSharpx

module TestMultiCommandInterpreter = 
    let log = createLogger "Eventful.Testing.TestMultiCommandInterpreter"

    let interpret 
        (prog : MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseType,'TMetadata>>)
        (runCommand : obj -> 'TCommandContext -> 'TState -> ('TState * CommandResult<'TBaseType,'TMetadata>))
        (eventStore : 'TState) 
        : 'TState = 

        let rec loop eventStore program : 'TState =
            match program with
            | FreeMultiCommand (RunCommand asyncBlock) ->
                let (cmd, cmdCtx, next) = asyncBlock |> Async.RunSynchronously
                let (eventStore', result) = runCommand cmd cmdCtx eventStore
                next result
                |> loop eventStore'
            | FreeMultiCommand (RunAsync (asyncBlock, next)) ->
                let result = asyncBlock |> Async.RunSynchronously
                next result
                |> loop eventStore
            | FreeMultiCommand (NotYetDone g) ->
                let next = g ()
                loop eventStore next 
            | Pure result ->
                eventStore

        loop eventStore prog 