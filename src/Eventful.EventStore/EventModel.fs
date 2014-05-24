namespace Eventful.EventStore

open System

open EventStore.ClientAPI
open Eventful
open FSharp.Control

type ISerializer = 
    abstract Serialize<'T> : 'T -> byte[]
    abstract DeserializeObj : byte[] -> string -> obj

type Message = 
|    Event of (obj * Map<string,seq<(string *  IStateBuilder * (obj -> seq<obj>))>>)

type EventModel (connection : IEventStoreConnection, config : EventProcessingConfiguration, serializer : ISerializer) =
    let log (msg : string) = Console.WriteLine(msg)

    let client = new Client(connection)

    let groupMessageIntoStream message =
        match message with
        | Event (event, handlerMap) ->
            handlerMap
            |> Map.toSeq
            |> Seq.map fst
            |> Set.ofSeq

    let getState streamId (stateBuilder : IStateBuilder) = 
        async {
            let fold state (event : ResolvedEvent) =
                let evt = serializer.DeserializeObj event.Event.Data event.Event.EventType
                stateBuilder.Fold state evt
            return! 
                client.readStreamForward streamId 
                |> AsyncSeq.fold fold stateBuilder.Zero
        }
        
    let processMessage streamId (stateBuilder : IStateBuilder) (handler : obj -> Choice<seq<obj>,_>) =
         async {
            let! state = getState streamId stateBuilder
            let result = handler state
            match result with
            | Choice1Of2 newEvents ->
                let eventData =
                    newEvents
                    |> Seq.map (fun x -> new EventData(Guid.NewGuid(),  config.TypeToTypeName (x.GetType()), true, serializer.Serialize(x), null))
                    |> Array.ofSeq
                do! client.append streamId EventStore.ClientAPI.ExpectedVersion.Any eventData
            | _ -> ()
                
            return result
        }

    let processEventList stream messages = async {
        let rec loop messages' = 
            match messages' with 
            | [] -> async { return () }
            | (_, sb, h : (obj -> seq<obj>))::xs -> async {
                let h' = h >> Choice1Of2
                do! processMessage stream sb h' |> Async.Ignore
                return! loop xs }

        do! loop <| (messages |> List.ofSeq)
    }

    let processMessages stream messages = async {
        let rec loop messages' =
            match messages' with
            | [] -> async { return () } 
            | x::xs ->
                match x with
                | Event (evt, handlers) ->
                    let handlersForThisStream = handlers |> Map.find stream
                    processEventList stream handlersForThisStream

        do! loop (messages |> List.ofSeq)
    }

    let queue = new WorktrackingQueue<_,_>(groupMessageIntoStream, processMessages)

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
                let processList = 
                    handlers
                    |> Seq.collect (fun h -> h evt)
                    |> Seq.toList
                    |> Seq.groupBy (fun (stream,_,_) -> stream)
                    |> Map.ofSeq

                do! queue.Add <| Event (evt, processList)
            | None -> ()
        }

    member x.RunCommand cmd streamId =
        let cmdKey = cmd.GetType().FullName
        match config.CommandHandlers |> Map.tryFind cmdKey with
        | Some (t,handler) -> 
            let (stream, stateBuilder, handler') = handler cmd
            let state = stateBuilder.Zero
            processMessage stream stateBuilder handler'
        | None -> 
            async { return Choice2Of2 (Seq.singleton <| sprintf "No handler for command: %A" cmdKey) }