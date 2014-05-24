namespace Eventful.EventStore

open FSharp.Control
open EventStore.ClientAPI
open System

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
        
    member x.readStreamBackward streamId =
        readStream streamId EventStore.ClientAPI.StreamPosition.End connection.ReadStreamEventsBackwardAsync

    member x.readStreamForward streamId =
        readStream streamId EventStore.ClientAPI.StreamPosition.Start connection.ReadStreamEventsForwardAsync

    member x.append streamId expectedVersion eventData = async {
            do! connection.AppendToStreamAsync(streamId, expectedVersion, eventData).ContinueWith((fun _ -> ())) |> Async.AwaitTask
        }

    member x.subscribe (start : Position option) (handler : Guid -> ResolvedEvent -> Async<unit>) = 
        let nullablePosition = match start with
                               | Some position -> Nullable.op_Implicit(position)
                               | None -> Nullable()
        connection.SubscribeToAllFrom(nullablePosition, false, (fun subscription event -> handler event.Event.EventId event |> Async.RunSynchronously )) |> ignore