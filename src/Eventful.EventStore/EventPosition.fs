namespace Eventful

open EventStore.ClientAPI

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventPosition = 

    let toEventStorePosition position = 
        new EventStore.ClientAPI.Position(position.Commit, position.Prepare)

    let ofEventStorePosition (position : Position) = 
        {
            Commit = position.CommitPosition
            Prepare = position.PreparePosition
        }