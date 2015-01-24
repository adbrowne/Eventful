namespace Eventful

open EventStore.ClientAPI
open System

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventPosition = 

    let toEventStorePosition position = 
        new EventStore.ClientAPI.Position(position.Commit, position.Prepare)

    let ofEventStorePosition (position : Position) = 
        {
            Commit = position.CommitPosition
            Prepare = position.PreparePosition
        }

type ContextStartData = {
    CorrelationId : Guid option
    ContextId : Guid
    Stopwatch : System.Diagnostics.Stopwatch
    Name : string
    ExtraTemplate : string
    ExtraVariables : obj []
}