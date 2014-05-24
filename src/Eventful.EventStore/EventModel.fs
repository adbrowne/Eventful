namespace Eventful.EventStore

open System

open EventStore.ClientAPI
open Eventful

type ISerializer = 
    abstract Serialize<'T> : 'T -> byte[]
    abstract DeserializeObj : byte[] -> Type -> obj

type EventModel (connection : IEventStoreConnection, config : EventProcessingConfiguration, serializer : ISerializer) =
    let log (msg : string) = Console.WriteLine(msg)

    let client = new Client(connection)
    let processMessage streamId (stateBuilder : IStateBuilder) (handler : obj -> seq<obj>) =
        async {
            let state = stateBuilder.Zero
            let result = handler state
            let eventData = 
                result
                |> Seq.map (fun x -> new EventData(Guid.NewGuid(), x.GetType().Name, true, serializer.Serialize(x), null))
                |> Array.ofSeq
            do!  client.append streamId EventStore.ClientAPI.ExpectedVersion.Any eventData
        }

    member x.Start (position : Position option) = 
        let nullablePosition = match position with
                               | Some position -> Nullable.op_Implicit(position)
                               | None -> Nullable()
        client.subscribe position x.EventAppeared

    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
        log <| sprintf "Received: %A: %A" eventId event.Event.EventType

        async {
            match config.EventHandlers |> Map.tryFind event.Event.EventType with
            | Some (t,handlers) -> 
                let evt = serializer.DeserializeObj (event.Event.Data) t
                do!
                    handlers
                    |> Seq.collect (fun h -> h evt)
                    |> Seq.map (fun (stream, stateBuilder, handler') -> processMessage stream stateBuilder handler')
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
                do! client.append streamId EventStore.ClientAPI.ExpectedVersion.Any eventDatas
                return result
            }
        | _ -> 
            async {
                return result
            }