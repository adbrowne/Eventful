namespace BookLibrary

open System
open Eventful
open BookLibrary.Aggregates

[<CLIMutable>]
type AddBookCopyCommand = {
    [<GeneratedIdAttribute>]BookCopyId : BookCopyId
    BookId : BookId
}

module BookCopy =
    let getStreamName () (bookCopyId : BookCopyId) =
        sprintf "BookCopy-%s" <| bookCopyId.Id.ToString("N")

    let getEventStreamName (context : UnitEventContext) (bookCopyId : BookCopyId) =
        sprintf "BookCopy-%s" <| bookCopyId.Id.ToString("N")

    let inline getBookCopyId (a: ^a) _ = 
        (^a : (member BookCopyId: BookCopyId) (a))

    let inline bookCopyCmdHandler f = 
        cmdHandler (fun x -> { BookCopyId.Id = x.AggregateId }) f

    let cmdHandlers = 
        seq {
           let addBookCopy (cmd : AddBookCopyCommand) =
               { 
                   BookCopyAddedEvent.BookCopyId = cmd.BookCopyId
                   BookId = cmd.BookId
               }

           yield bookCopyCmdHandler addBookCopy
        }

    let bookCopyIdGuid (bookCopyId : BookCopyId) = bookCopyId.Id

    let handlers () =
        Eventful.Aggregate.toAggregateDefinition getStreamName getEventStreamName bookCopyIdGuid cmdHandlers Seq.empty

open System.Web
open System.Net.Http
open System.Web.Http
open System.Web.Http.Routing
open FSharpx.Choice
open Eventful
open FSharpx.Collections

[<RoutePrefix("api/bookcopies")>]
type BookCopiesController(system : IBookLibrarySystem) =
    inherit ApiController()
 
    // POST /api/values
    [<Route("")>]
    [<HttpPost>]
    member x.Post (cmd:AddBookCopyCommand) = 
        async {
            let bookCopyId = BookCopyId.New()
            let cmdWithId = { cmd with BookCopyId = bookCopyId }
            let! cmdResult = system.RunCommand cmdWithId 
            return
                match cmdResult with
                | Choice1Of2 result ->
                     let responseBody = new Newtonsoft.Json.Linq.JObject();
                     responseBody.Add("bookCopyId", new Newtonsoft.Json.Linq.JValue(bookCopyId.Id))
                     let response = x.Request.CreateResponse<Newtonsoft.Json.Linq.JObject>(Net.HttpStatusCode.Accepted, responseBody)
                     match result.Position with
                     | Some position ->
                         response.Headers.Add("eventful-last-write", position.BuildToken())
                     | None ->
                         ()
                     response
                | Choice2Of2 errorResult ->
                     let response = x.Request.CreateResponse<NonEmptyList<CommandFailure>>(Net.HttpStatusCode.BadRequest, errorResult)
                     response
        } |> Async.StartAsTask