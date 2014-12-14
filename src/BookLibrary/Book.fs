namespace BookLibrary

open System
open BookLibrary.Aggregates
open Eventful
open FSharpx.Validation
open FSharpx.Choice
open FSharpx
open FSharpx.Collections

[<CLIMutable>]
type AddBookCommand = {
    [<GeneratedIdAttribute>]BookId : BookId
    Title : string
}

[<CLIMutable>]
type BookAddedEvent = 
    { BookId : BookId
      Title : string }
    interface IBookEvent with
      member x.BookId = x.BookId

[<CLIMutable>]
type UpdateBookTitleCommand = {
    [<FromRoute>] BookId : BookId
    Title : string
}

[<CLIMutable>]
type BookTitleUpdatedEvent = 
    { BookId : BookId
      Title : string }
    interface IBookEvent with
      member x.BookId = x.BookId

module Book =
    let addBookEventHandler (f : ('s * #IBookEvent * BookLibraryEventMetadata) -> 's) s =
        s |> StateBuilder.handler (fun evt _ -> evt :> IBookEvent) f

    let getStreamName () (bookId : BookId) =
        sprintf "Book-%s" <| bookId.Id.ToString("N")

    let getEventStreamName (context : BookLibraryEventContext) (bookId : BookId) =
        sprintf "Book-%s" <| bookId.Id.ToString("N")

    let inline getBookId (a: ^a) _ = 
        (^a : (member BookId: BookId) (a))

    let bookTitle = 
        StateBuilder.Empty "bookTitle" ""
        |> addBookEventHandler (fun (_, (e : BookAddedEvent), _) -> e.Title)
        |> addBookEventHandler (fun (_, (e : BookTitleUpdatedEvent), _) -> e.Title)

    let copyCount =
        StateBuilder.Empty "bookCopyCount" 0
        |> addBookEventHandler (fun (s, (e : BookCopyAddedEvent), _) -> s + 1)

    let doesNotEqual err other value =
        if other = value then
            err
            |> NonEmptyList.singleton 
            |> Choice2Of2
        else
            Choice1Of2 value

    let buildBookMetadata = 
        Aggregates.emptyMetadata AggregateType.Book

    let inline bookCmdHandlerS stateBuilder f = 
        cmdHandlerS stateBuilder f buildBookMetadata

    let inline bookCmdHandler f =
        cmdHandler f buildBookMetadata

    let addMetadata result =
       result
       |> Choice.map (fun events ->
           let uniqueId = Guid.NewGuid().ToString()

           {
                UniqueId = uniqueId
                Events = events |> List.map (fun evt -> (evt, buildBookMetadata))
           }
       )

    let cmdHandlers = 
        seq {
           yield 
               (fun (cmd : AddBookCommand) ->
               { 
                   BookAddedEvent.BookId = cmd.BookId
                   Title = cmd.Title
               })
               |> bookCmdHandler 

           let updateTitleHandler currentTitle () (cmd : UpdateBookTitleCommand) = 
               let updateTitle newTitle =
                   [{
                       BookId = cmd.BookId
                       Title = newTitle
                   } :> IEvent]

               let newTitle = doesNotEqual (Some "title", "Cannot update title to the same value") currentTitle cmd.Title

               updateTitle <!> newTitle
               |> addMetadata 
           
           yield AggregateActionBuilder.fullHandler bookTitle updateTitleHandler
                 |> AggregateActionBuilder.buildCmd
        }

    let eventHandlers =
        seq {
            yield linkEvent (fun (evt : BookCopyAddedEvent) -> evt.BookId) buildBookMetadata

            let onBookAwarded bookCopyCount (evt : BookPrizeAwardedEvent) = seq {
                if(bookCopyCount > 10) then
                    yield ({ BookPromotedEvent.BookId = evt.BookId } :> IEvent, buildBookMetadata)
            }

            yield onEvent (fun (evt : BookPrizeAwardedEvent) _ -> evt.BookId) copyCount onBookAwarded
        }

    let handlers () =
        Eventful.Aggregate.toAggregateDefinition 
            AggregateType.Book 
            BookLibraryEventMetadata.GetUniqueId
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            eventHandlers

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
        |> DocumentBuilder.mapStateToProperty bookTitle (fun e -> e.BookId) (fun doc -> doc.Title) (fun value doc -> { doc with Title = value })

open Suave
open Suave.Http
open Suave.Http.Applicatives
open BookLibrary.WebHelpers

module BooksWebApi = 
    let addHandler (cmd : AddBookCommand) =
        let cmd = { cmd with BookId = { BookId.Id = Guid.NewGuid() }}
        let successResponse = 
            let responseBody = new Newtonsoft.Json.Linq.JObject();
            responseBody.Add("bookId", new Newtonsoft.Json.Linq.JValue(cmd.BookId.Id))
            responseBody
        (cmd, successResponse :> obj)

    let updateHandler bookId (cmd : UpdateBookTitleCommand) =
        let cmd = { cmd with BookId = { BookId.Id =  bookId}}
        let successResponse = new Newtonsoft.Json.Linq.JObject();
        (cmd, successResponse :> obj)

    let config system =
        choose [
            url "/api/books" >>= choose
                [ 
                    POST >>= commandHandler system addHandler
                ]
            url_with_guid "/api/book/{id}/title" (fun (id:Guid) -> 
                choose
                    [ 
                        PUT >>= commandHandler system (updateHandler id)
                    ])
        ]
