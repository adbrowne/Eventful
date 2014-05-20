namespace Eventful.EventStore

open System

open EventStore.ClientAPI
open Eventful

type ISerializer = 
    abstract Serialize<'T> : 'T -> byte[]
    abstract DeserializeObj : byte[] -> Type -> obj

type EventModel (connection : IEventStoreConnection, config : EventProcessingConfiguration, serializer : ISerializer) =
    let log (msg : string) = Console.WriteLine(msg)

    member x.Start (position : Position option) = 
        let nullablePosition = match position with
                               | Some position -> Nullable.op_Implicit(position)
                               | None -> Nullable()
        connection.SubscribeToAllFrom(nullablePosition, false, (fun subscription event -> x.EventAppeared event event.Event.EventId |> Async.RunSynchronously ))

    member x.EventAppeared (event : ResolvedEvent) eventId : Async<unit> =
        log <| sprintf "Received: %A: %A" eventId event.Event.EventType

        async {
            match config.EventHandlers |> Map.tryFind event.Event.EventType with
            | Some (t,handlers) -> 
                let evt = serializer.DeserializeObj (event.Event.Data) t
                do!
                    handlers
                    |> Seq.collect (fun h -> h evt)
                    |> Seq.map (fun (stream, stateBuilder, handler') -> 
                        async {
                            let state = stateBuilder.Zero
                            let result = handler' state
                            let eventData = 
                                result
                                |> Seq.map (fun x -> new EventData(Guid.NewGuid(), x.GetType().Name, true, serializer.Serialize(x), null))
                                |> Array.ofSeq
                            do! connection.AppendToStreamAsync(stream, EventStore.ClientAPI.ExpectedVersion.Any, eventData).ContinueWith((fun _ -> ())) |> Async.AwaitTask  
                        }
                    )
                    |> Async.Parallel |> Async.Ignore
            | None -> ()
        }

    member x.RunCommand cmd streamId =
        let cmdKey = cmd.GetType().FullName
        let result =
            match config.CommandHandlers |> Map.tryFind cmdKey with
            | Some (t,handler) -> 
                let (stream, stateBuilder, handler') = handler cmd
                let state = stateBuilder.Zero
                handler' state
            | None -> 
                Choice2Of2 (Seq.singleton <| sprintf "No handler for command: %A" cmdKey)
        match result with
        | Choice1Of2 events ->
            let eventDatas = 
                events
                |> Seq.map (fun e -> new EventData(Guid.NewGuid(), e.GetType().Name, true, serializer.Serialize e, null)) 
                |> Seq.toArray

            async {
                do! connection.AppendToStreamAsync(streamId, EventStore.ClientAPI.ExpectedVersion.NoStream, eventDatas).ContinueWith((fun _ -> true)) |> Async.AwaitTask |> Async.Ignore
                return result
            }
        | _ -> 
            async {
                return result
            }
