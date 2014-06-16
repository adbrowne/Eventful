namespace Eventful.EventStore

open FSharp.Control
open EventStore.ClientAPI
open System
open FSharpx.Collections

/// simple F# wrapper around EventStore functions
type Client (connection : IEventStoreConnection) =
    let readStream 
        streamId 
        startPosition 
        (readAsync : (string * int * int * bool) -> System.Threading.Tasks.Task<StreamEventsSlice>) =
        let rec loop next =
            asyncSeq {
                let! events = readAsync(streamId, next, 100, false) |> Async.AwaitTask
                for evt in events.Events do
                    yield evt
                if events.IsEndOfStream then
                    ()
                else
                    yield! loop events.NextEventNumber
            }
        loop startPosition

    member x.readEvent streamId eventNumber =
        connection.ReadEventAsync(streamId, eventNumber, false) |> Async.AwaitTask
        
    member x.readStreamBackward streamId =
        readStream streamId EventStore.ClientAPI.StreamPosition.End connection.ReadStreamEventsBackwardAsync

    member x.readStreamForward streamId from =
        readStream streamId from connection.ReadStreamEventsForwardAsync

    member x.readStreamHead streamId = async {
        let! (result : StreamEventsSlice) = connection.ReadStreamEventsBackwardAsync(streamId, EventStore.ClientAPI.StreamPosition.End, 1, false) |> Async.AwaitTask
        return result.Events |> Seq.tryHead
    }

    member x.append streamId expectedVersion eventData = async {
            do! connection.AppendToStreamAsync(streamId, expectedVersion, eventData).ContinueWith((fun _ -> true)) |> Async.AwaitTask |> Async.Ignore
        }

    member x.getNextPosition () = async {
        let position = Position.End
        let! (finalSlice : AllEventsSlice) = connection.ReadAllEventsBackwardAsync(position, 1, false, null)
        let nextPosition = finalSlice.NextPosition
        return nextPosition }

    member x.ensureMetadata streamId (data : StreamMetadata) = async {
        let! (metadata : StreamMetadataResult) = (connection.GetStreamMetadataAsync(streamId) |> Async.AwaitTask)
        if (metadata.MetastreamVersion = ExpectedVersion.EmptyStream) then
            try 
                do! ((connection.SetStreamMetadataAsync(streamId, ExpectedVersion.EmptyStream, data).ContinueWith((fun x -> true))) |> Async.AwaitTask |> Async.Ignore)
            with
            | :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException -> return ()
        else
            return ()
    }

    member x.subscribe (start : Position option) (handler : Guid -> ResolvedEvent -> Async<unit>) = 
        let nullablePosition = match start with
                               | Some position -> Nullable.op_Implicit(position)
                               | None -> Nullable()
        connection.SubscribeToAllFrom(nullablePosition, false, (fun subscription event -> handler event.Event.EventId event |> Async.RunSynchronously ))