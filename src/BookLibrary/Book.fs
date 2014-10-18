namespace BookLibrary

open System
open BookLibrary.Aggregates

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

type Event = Event of obj

module Book =
    let getStreamName () (bookId : BookId) =
        sprintf "Book-%s" <| bookId.Id.ToString("N")

    let cmdHandlers = 
        seq {
           let addBook (cmd : AddBookCommand) =
               Event { 
                   BookAddedEvent.BookId = cmd.BookId
                   Title = cmd.Title
               }

           yield buildCmdHandler addBook
        }

    let bookIdGuid (bookId : BookId) = bookId.Id
    let handlers =
        toAggregateDefinition getStreamName getStreamName bookIdGuid cmdHandlers Seq.empty