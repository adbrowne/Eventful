namespace BookLibrary

open System.Threading.Tasks
open Eventful

type IBookLibrarySystem =
    abstract member RunCommand<'a> : 'a -> Async<CommandResult<BookLibraryEventMetadata>>
    abstract member RunCommandTask<'a> : 'a -> Task<CommandResult<BookLibraryEventMetadata>>