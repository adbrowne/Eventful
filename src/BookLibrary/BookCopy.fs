namespace BookLibrary

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

    let buildBookCopyMetadata = 
        Aggregates.emptyMetadata AggregateType.BookCopy

    let inline bookCopyCmdHandler f = 
        cmdHandler f buildBookCopyMetadata

    let cmdHandlers = 
        seq {
           let addBookCopy (cmd : AddBookCopyCommand) =
               { 
                   BookCopyAddedEvent.BookCopyId = cmd.BookCopyId
                   BookId = cmd.BookId
               }

           yield bookCopyCmdHandler addBookCopy
        }

    let handlers () =
        Eventful.Aggregate.toAggregateDefinition 
            AggregateType.BookCopy 
            BookLibraryEventMetadata.GetUniqueId
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            Seq.empty

open Suave.Http
open Suave.Http.Applicatives
open BookLibrary.WebHelpers

module BooksCopiesWebApi = 
    let addHandler (cmd : AddBookCopyCommand) =
        let bookCopyId = BookCopyId.New()
        let cmd = { cmd with BookCopyId = bookCopyId }
        let successResponse = 
            let responseBody = new Newtonsoft.Json.Linq.JObject();
            responseBody.Add("bookCopyId", new Newtonsoft.Json.Linq.JValue(bookCopyId))
            responseBody
        (cmd, successResponse :> obj)

    let config system =
        choose [
            url "/api/bookcopies" >>= choose
                [ 
                    POST >>= commandHandler system addHandler
                ]
        ]