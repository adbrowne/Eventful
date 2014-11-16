namespace BookLibrary

open System.Threading.Tasks
open Eventful
open Eventful.EventStore
open System

type IBookLibrarySystem =
    abstract member RunCommand<'a> : 'a -> Async<CommandResult<obj,BookLibraryEventMetadata>>
    abstract member RunCommandTask<'a> : 'a -> Task<CommandResult<obj,BookLibraryEventMetadata>>

type BookLibraryEventStoreSystem = EventStoreSystem<unit,UnitEventContext,BookLibraryEventMetadata,obj,AggregateType>

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
type DeliveryId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type FileId = {
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

[<CLIMutable>]
type DeliveryAcceptedEvent = {
    DeliveryId : DeliveryId
    FileId : FileId
}