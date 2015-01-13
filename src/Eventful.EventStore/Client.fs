namespace Eventful.EventStore

open FSharp.Control
open EventStore.ClientAPI
open System
open FSharpx
open Eventful

/// simple F# wrapper around EventStore functions
type Client (connection : IEventStoreConnection) =
    let log = createLogger "Eventful.EventStore.Client"

    let readSlice
        streamId 
        startPosition 
        maxItems
        (readAsync : (string * int * int * bool) -> System.Threading.Tasks.Task<StreamEventsSlice>) =
        async {
            let! slice = readAsync(streamId, startPosition, maxItems, true) |> Async.AwaitTask
            return slice.Events
        }

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

    member x.readStreamSliceForward streamId from maxItems =
        readSlice streamId from maxItems connection.ReadStreamEventsForwardAsync

    member x.readEventFromPosition position = async {
        let! slice = connection.ReadAllEventsForwardAsync(EventPosition.toEventStorePosition position, 1, true) |> Async.AwaitTask
        return 
            match slice.Events |> List.ofArray with
            | [] -> None
            | [evt] -> Some evt
            | _ -> failwith "Expecting only one event to be returned"
    }

    member x.readStreamHead streamId = async {
        let! (result : StreamEventsSlice) = connection.ReadStreamEventsBackwardAsync(streamId, EventStore.ClientAPI.StreamPosition.End, 1, false) |> Async.AwaitTask
        return (result.Events |> tryHead, result.FromEventNumber)
    }

    member x.append streamId expectedVersion eventData = async {
        try
            let! result = connection.AppendToStreamAsync(streamId, expectedVersion, eventData) |> Async.AwaitTask
            log.Debug <| lazy(sprintf "Wrote %A %A %A" streamId expectedVersion (eventData |> Seq.map (fun x -> x.Type) |> Seq.toArray |> (fun x -> String.Join(",", x))))

            return WriteResult.WriteSuccess (EventPosition.ofEventStorePosition result.LogPosition)
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

    member x.writeStreamMetadata streamId (streamMetadata : StreamMetadata) = 
        connection.SetStreamMetadataAsync(streamId, ExpectedVersion.Any, streamMetadata) 
        |> Async.AwaitTask
        |> Async.map (konst ())

    member x.getStreamMetadata streamId = 
        connection.GetStreamMetadataAsync streamId |> Async.AwaitTask

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
                               | None -> Nullable.op_Implicit(Position.Start)

        let onEventHandler (event : ResolvedEvent) =
            try
                handler event.Event.EventId event
                |> Async.RunSynchronously
            with | exn ->
                log.ErrorWithException(lazy(sprintf "Exception thrown by subscription event handler", exn))

        log.Debug <| lazy(sprintf "Subscription starting at: %A" nullablePosition)
        connection.SubscribeToAllFrom(nullablePosition, false, (fun _ event -> onEventHandler event), (fun _ -> onLive ()), (fun _ reason exn -> log.Debug <| lazy(sprintf "Dropped %A %A" reason exn)))