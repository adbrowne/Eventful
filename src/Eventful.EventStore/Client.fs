namespace Eventful.EventStore

open FSharp.Control
open EventStore.ClientAPI
open System
open FSharpx.Collections
open Eventful

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

    member x.Connect () =
        connection.ConnectAsync().ContinueWith(fun x -> true) |> Async.AwaitTask |> Async.Ignore

    member x.readEvent streamId eventNumber =
        connection.ReadEventAsync(streamId, eventNumber, true) |> Async.AwaitTask
        
    member x.readStreamBackward streamId =
        readStream streamId EventStore.ClientAPI.StreamPosition.End connection.ReadStreamEventsBackwardAsync

    member x.readStreamForward streamId from =
        readStream streamId from connection.ReadStreamEventsForwardAsync

    member x.readStreamHead streamId = async {
        let! (result : StreamEventsSlice) = connection.ReadStreamEventsBackwardAsync(streamId, EventStore.ClientAPI.StreamPosition.End, 1, false) |> Async.AwaitTask
        return result.Events |> Seq.tryHead
    }

    member x.append streamId expectedVersion eventData = async {
        try
            let! result = connection.AppendToStreamAsync(streamId, expectedVersion, eventData) |> Async.AwaitTask
            return WriteResult.WriteSuccess
        with 
            | :? AggregateException as ex ->
                if(ex.InnerException <> null && ex.InnerException.GetType() = typeof<EventStore.ClientAPI.Exceptions.WrongExpectedVersionException>) then
                    return WriteResult.WrongExpectedVersion
                else
                    return WriteResult.WriteError ex 
            | ex -> return WriteResult.WriteError ex
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
                do! connection.SetStreamMetadataAsync(streamId, ExpectedVersion.EmptyStream, data) |> Async.AwaitTask |> Async.Ignore
            with
            | :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException -> return ()
        else
            return ()
    }

    member x.subscribe (start : Position option) (handler : Guid -> ResolvedEvent -> Async<unit>) (onLive : (unit -> unit)) =
        let nullablePosition = match start with
                               | Some position -> Nullable.op_Implicit(position)
                               | None -> Nullable()

        let onEventHandler (event : ResolvedEvent) =
            handler event.Event.EventId event
            |> Async.RunSynchronously

        connection.SubscribeToAllFrom(nullablePosition, false, (fun _ event -> onEventHandler event), (fun _ -> onLive ()))