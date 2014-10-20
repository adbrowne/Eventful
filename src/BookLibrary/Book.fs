namespace BookLibrary

open System
open BookLibrary.Aggregates
open Eventful
open FSharpx.Validation
open FSharpx.Choice
open FSharpx.Collections

type BookId = {
    Id : Guid
}

[<CLIMutable>]
type AddBookCommand = {
    BookId : BookId
    Title : string
}

[<CLIMutable>]
type BookAddedEvent = {
    BookId : BookId
    Title : string
}

[<CLIMutable>]
type UpdateBookTitleCommand = {
    BookId : BookId
    Title : string
}

[<CLIMutable>]
type BookTitleUpdatedEvent = {
    BookId : BookId
    Title : string
}

type BookEvents = 
    | Added of BookAddedEvent
    | TitleUpdated of BookTitleUpdatedEvent

module Book =
    let getStreamName () (bookId : BookId) =
        sprintf "Book-%s" <| bookId.Id.ToString("N")

    let inline getBookId (a: ^a) _ = 
        (^a : (member BookId: BookId) (a))

    let validateCommand (cmd : AddBookCommand) : seq<string option * string> =
       match (Choice1Of2 "andrew") with
       | Choice1Of2 _ -> Seq.empty
       | Choice2Of2 errors -> errors |> FSharpx.Collections.NonEmptyList.toSeq

    let bookTitle = 
        StateBuilder.Empty "bookTitle" ""
        |> StateBuilder.handler getBookId (fun (_, (e : BookAddedEvent), _) -> e.Title)
        |> StateBuilder.handler getBookId (fun (_, (e : BookTitleUpdatedEvent), _) -> e.Title)

    let doesNotEqual err other value =
        if other = value then
            err
            |> NonEmptyList.singleton 
            |> Choice2Of2
        else
            Choice1Of2 value

    let cmdHandlers = 
        seq {
           let addBook (cmd : AddBookCommand) =
               Added { 
                   BookAddedEvent.BookId = cmd.BookId
                   Title = cmd.Title
               }

           yield buildCmdHandler addBook

           yield fullHandler bookTitle (fun currentTitle m (cmd : UpdateBookTitleCommand) ->
               let updateTitle newTitle =
                   [TitleUpdated {
                       BookId = cmd.BookId
                       Title = newTitle
                   }]

               let newTitle = doesNotEqual (Some "title", "Cannot update title to the same value") currentTitle cmd.Title

               updateTitle <!> newTitle
           )
        }

    let bookIdGuid (bookId : BookId) = bookId.Id
    let handlers () =
        Eventful.Aggregate.toAggregateDefinition getStreamName getStreamName bookIdGuid cmdHandlers Seq.empty

    type BookDocument = {
        BookId : Guid
        Title : string
    }
    with static member NewDoc bookId = {
            BookId = bookId
            Title = ""
        }


    let documentBuilder : DocumentBuilder<BookId, BookDocument, BookLibraryEventMetadata> = 
        DocumentBuilder.Empty<BookId, BookDocument> (fun x -> BookDocument.NewDoc x.Id) (fun x -> sprintf "Book/%s" (x.Id.ToString()))
        |> DocumentBuilder.mapStateToProperty bookTitle (fun doc -> doc.Title) (fun value doc -> { doc with Title = value })

open System.Web
open System.Net.Http
open System.Web.Http
 
type BooksController() =
    inherit ApiController()
 
    // GET /api/values
    member x.Get() = [| "value1"; "value2" |] |> Array.toSeq

    // GET /api/values/5
    member x.Get (id:int) = "value"

    // POST /api/values
    member x.Post ([<FromBody>] value:string) = ()

    // PUT /api/values/5
    member x.Put (id:int) ([<FromBody>] value:string) = ()

    // DELETE /api/values/5
    member x.Delete (id:int) = ()