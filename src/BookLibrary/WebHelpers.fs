namespace BookLibrary

open Suave
open Suave.Http
open Suave.Http.Successful

module WebHelpers =
    let fromJson<'TDto> (f : 'TDto -> Types.WebPart) (context : Types.HttpContext) : Types.SuaveTask<Types.HttpContext> =
        let dto = Serialization.deserializeObj context.request.raw_form typeof<'TDto> :?> 'TDto
        f dto context 
        
    let runCommandInSystem (system  : IBookLibrarySystem) (cmd, successResult) (context : Types.HttpContext) : Types.SuaveTask<Types.HttpContext> = async {
        let! result = system.RunCommand cmd 
        return!
            match result with
            | Choice1Of2 result ->
                match result.Position with
                | Some position -> 
                    Writers.set_header "eventful-last-write" (position.BuildToken()) 
                    >>= 
                        (Serialization.serialize successResult |> accepted)
                | None -> 
                    Serialization.serialize successResult
                    |> accepted
            | Choice2Of2 errorResult ->
                Serialization.serialize errorResult
                |> Suave.Http.RequestErrors.bad_request
            |> (fun x -> x context)
    }
        
    let commandHandler<'TCommand> (system : IBookLibrarySystem) (f : 'TCommand -> ('TCommand * obj)) : Types.WebPart =
        fromJson<'TCommand> (f >> runCommandInSystem system)