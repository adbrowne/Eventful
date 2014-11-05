namespace BookLibrary

open System
open BookLibrary.Aggregates
open Eventful
open FSharpx.Validation
open FSharpx.Choice
open FSharpx.Collections

[<CLIMutable>]
type AddBookCommand = {
    [<GeneratedIdAttribute>]BookId : BookId
    Title : string
}

[<CLIMutable>]
type BookAddedEvent = {
    BookId : BookId
    Title : string
}

[<CLIMutable>]
type UpdateBookTitleCommand = {
    [<FromRoute>] BookId : BookId
    Title : string
}

type IEvent = interface end

[<CLIMutable>]
type BookTitleUpdatedEvent = {
    BookId : BookId
    Title : string 
}
with interface IEvent

module Book =
    let getStreamName () (bookId : BookId) =
        sprintf "Book-%s" <| bookId.Id.ToString("N")

    let getEventStreamName (context : UnitEventContext) (bookId : BookId) =
        sprintf "Book-%s" <| bookId.Id.ToString("N")

    let inline getBookId (a: ^a) _ = 
        (^a : (member BookId: BookId) (a))

    let bookTitle = 
        StateBuilder.Empty "bookTitle" ""
        |> StateBuilder.handler getBookId (fun (_, (e : BookAddedEvent), _) -> e.Title)
        |> StateBuilder.handler getBookId (fun (_, (e : BookTitleUpdatedEvent), _) -> e.Title)

    let copyCount =
        StateBuilder.Empty "bookCopyCount" 0
        |> StateBuilder.handler getBookId (fun (s, (e : BookCopyAddedEvent), _) -> s + 1)

    let doesNotEqual err other value =
        if other = value then
            err
            |> NonEmptyList.singleton 
            |> Choice2Of2
        else
            Choice1Of2 value

    let getBookIdFromMetadata = (fun (x : BookLibraryEventMetadata) -> { BookId.Id = x.AggregateId })

    let inline buildBookMetadata (bookId : BookId) = 
        Aggregates.emptyMetadata bookId.Id

    let inline bookCmdHandlerS stateBuilder f = 
        cmdHandlerS getBookIdFromMetadata stateBuilder f (fun bookId -> Aggregates.emptyMetadata bookId.Id)

    let inline bookCmdHandler f =
        cmdHandler getBookIdFromMetadata f (fun bookId -> Aggregates.emptyMetadata bookId.Id)

    let cmdHandlers = 
        seq {
           yield 
               (fun (cmd : AddBookCommand) ->
               { 
                   BookAddedEvent.BookId = cmd.BookId
                   Title = cmd.Title
               })
               |> bookCmdHandler 

           yield 
               (fun (cmd : AddBookCommand) ->
               { 
                   BookAddedEvent.BookId = cmd.BookId
                   Title = cmd.Title
               })
               |> bookCmdHandler 

           yield 
               (fun currentTitle m (cmd : UpdateBookTitleCommand) ->
                   let updateTitle newTitle =
                       [{
                           BookId = cmd.BookId
                           Title = newTitle
                       } :> obj]

                   let newTitle = doesNotEqual (Some "title", "Cannot update title to the same value") currentTitle cmd.Title

                   updateTitle <!> newTitle
               )
               |> bookCmdHandlerS bookTitle 
        }

    let eventHandlers =
        seq {
            yield linkEvent (fun (evt : BookCopyAddedEvent) -> evt.BookId) buildBookMetadata

            let onBookAwarded bookCopyCount (evt : BookPrizeAwardedEvent) = seq {
                if(bookCopyCount > 10) then
                    yield ({ BookPromotedEvent.BookId = evt.BookId } :> obj, buildBookMetadata)
            }

            yield onEvent (fun (evt : BookPrizeAwardedEvent) _ -> evt.BookId) copyCount onBookAwarded
        }

    let handlers () =
        Eventful.Aggregate.toAggregateDefinition getStreamName getEventStreamName cmdHandlers eventHandlers

    type BookDocument = {
        BookId : Guid
        Title : string
    }

    with static member NewDoc (bookId : BookId) = {
            BookId = bookId.Id
            Title = ""
        }

    let documentBuilder : DocumentBuilder<BookId, BookDocument, BookLibraryEventMetadata> = 
        DocumentBuilder.Empty<BookId, BookDocument> BookDocument.NewDoc (fun x -> sprintf "Book/%s" (x.Id.ToString()))
        |> DocumentBuilder.mapStateToProperty bookTitle (fun doc -> doc.Title) (fun value doc -> { doc with Title = value })

open System.Web
open System.Net.Http
open System.Web.Http
open System.Web.Http.Routing

[<RoutePrefix("api/books")>]
type BooksController(system : IBookLibrarySystem) =
    inherit ApiController()
 
    // POST /api/values
    [<Route("")>]
    [<HttpPost>]
    member x.Post (cmd:AddBookCommand) = 
        async {
            let cmdWithId = { cmd with BookId = { BookId.Id = Guid.NewGuid() }}
            let! cmdResult = system.RunCommand cmdWithId 
            return
                match cmdResult with
                | Choice1Of2 result ->
                     let responseBody = new Newtonsoft.Json.Linq.JObject();
                     responseBody.Add("bookId", new Newtonsoft.Json.Linq.JValue(cmdWithId.BookId.Id))
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

    // PUT /api/values/5
    [<Route("{bookId}/title")>]
    [<HttpPut>]
    member x.Put (bookId:Guid) ([<FromBody>] (cmd:UpdateBookTitleCommand)) = 
        async {
            let cmdWithId = { cmd with BookId = { BookId.Id = bookId }}
            let! cmdResult = system.RunCommand cmdWithId 
            return
                match cmdResult with
                | Choice1Of2 result ->
                     let responseBody = new Newtonsoft.Json.Linq.JObject();
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