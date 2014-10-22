namespace BookLibrary

open System.Threading.Tasks
open Eventful
open System

type IBookLibrarySystem =
    abstract member RunCommand<'a> : 'a -> Async<CommandResult<BookLibraryEventMetadata>>
    abstract member RunCommandTask<'a> : 'a -> Task<CommandResult<BookLibraryEventMetadata>>

[<CLIMutable>]
type BookId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type BookCopyId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type AwardId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type BookCopyAddedEvent = {
    BookCopyId : BookCopyId
    BookId : BookId
}

[<CLIMutable>]
type BookPromotedEvent = {
    BookId : BookId
}

[<CLIMutable>]
type BookPrizeAwardedEvent = {
    AwardId : AwardId
    BookId : BookId
}