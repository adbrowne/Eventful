namespace BookLibrary

open System
open Eventful
open Suave
open Suave.Http
open Suave.Http.Successful

module WebHelpers =
    let log = createLogger "BookLibrary.WebHelpers"
    let fromJson<'TDto> (f : 'TDto -> Types.WebPart) (context : Types.HttpContext) : Types.SuaveTask<Types.HttpContext> =
        let dto = Serialization.deserializeObj context.request.raw_form typeof<'TDto> :?> 'TDto
        f dto context 
        
    let runCommandInSystem (system  : IBookLibrarySystem) (cmd, successResult) (context : Types.HttpContext) : Types.SuaveTask<Types.HttpContext> = async {
        let! result = system.RunCommand cmd 
        log.RichDebug "Command Result {@Command} {@Result}" [|cmd;result|]
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
        
    let commandHandler (system : IBookLibrarySystem) (f : 'TCommand -> ('TCommand * obj)) : Types.WebPart =
        fromJson<'TCommand> (f >> runCommandInSystem system)

    let F prefix postfix h (r:Types.HttpContext) =
        let url = r.request.url
        match (url.StartsWith(prefix) && url.EndsWith(postfix)) with
        | true ->
          let idStart = prefix.Length
          let idLength = url.Length - postfix.Length - prefix.Length
          let idString = url.Substring(idStart, idLength)
          let (parses, guid) = Guid.TryParse idString
          if parses then
              let part = h guid
              part r
          else
              fail
        | false -> 
          fail

    let url_with_guid (pattern : String) =

        let idStartIndex = pattern.IndexOf("{id}")
        match idStartIndex with
        | -1 -> (fun _ -> never)
        | _ ->
            (fun (h : Guid -> Types.WebPart) ->
                let prefix = pattern.Substring(0, idStartIndex)
                let postfix = pattern.Substring(idStartIndex + 4, pattern.Length - (idStartIndex + 4))
                
                F prefix postfix h
            )
        
open Xunit
open FsUnit.Xunit

module WebHelperTests =
    let testUrlAgainstGuidRule pattern url =
         let handler = WebHelpers.url_with_guid pattern (fun guid -> OK (guid.ToString()))
         let request = Types.HttpRequest.mk "1.1" url "GET" List.empty String.Empty Log.TraceHeader.empty false Net.IPAddress.Loopback
         let context = Types.HttpContext.mk request Types.HttpRuntime.empty
         let result = handler context |> Async.RunSynchronously
         match result with
         | Some result ->
            result.response.content
            |> function
             | Types.HttpContent.Bytes bytes ->
                System.Text.Encoding.UTF8.GetString bytes
             | _ -> "Content was not bytes"
             |> Some
         | None ->
            None
        
    [<Fact>]
    let ``Given pattern contains id When url has Guid in the correct place Then Guid is returned`` () : unit =
         let guid = "5e33ad64-9943-46b8-9d58-d56a5de6f818"
         let url = sprintf "/a/%s/b" guid

         testUrlAgainstGuidRule "/a/{id}/b" url
         |> should equal (Some guid)

    [<Fact>]
    let ``Given pattern contains id When url does not have a valid guid in the id place Then rule does not match`` () : unit =
         let url = "/a/NOT_A_GUID/b"

         testUrlAgainstGuidRule "/a/{id}/b" url
         |> should equal None