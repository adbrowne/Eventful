namespace Eventful.EventStore

// TODO: REMOVE
//open System
//
//open EventStore.ClientAPI
//open Eventful
//open FSharp.Control
//open FSharp.Data
//open FSharpx
//
//type Message = 
//|    Event of (obj * Map<string,seq<(string *  IStateBuilder<obj,obj> * (obj -> seq<obj>))>> * EventPosition)
//
//type EventModel (connection : IEventStoreConnection, config : EventProcessingConfiguration, serializer : ISerializer) =
//    let toGesPosition position = new EventStore.ClientAPI.Position(position.Commit, position.Prepare)
//    let toEventfulPosition (position : Position) = { Commit = position.CommitPosition; Prepare = position.PreparePosition }
//
//    let log (msg : string) = Console.WriteLine(msg)
//
//    let client = new Client(connection)
//
//    let completeTracker = new LastCompleteItemAgent<EventPosition>()
//
//    let groupMessageIntoStream message =
//        match message with
//        | Event (event, handlerMap, _) ->
//            let keys = 
//                handlerMap
//                |> Map.toSeq
//                |> Seq.map fst
//                |> Set.ofSeq
//            (message, keys)
//
//    let getSnapshotStream stream (stateBuilder : IStateBuilder<_,_>) =
//        sprintf "%s-%s-%s" stream stateBuilder.Name stateBuilder.Version
//
//    let getState streamId (stateBuilder : IStateBuilder<obj,obj>) = 
//        async {
//            let types = 
//                stateBuilder.Types 
//                |> Seq.map config.TypeToTypeName
//                |> Set.ofSeq
//
//            let snapshotStream = getSnapshotStream streamId stateBuilder
//            let snapshot = client.readStreamBackward snapshotStream |> AsyncSeq.take 1 |> Seq.ofAsyncSeq |> List.ofSeq
//
//            let (startIndex, zero) =
//                match snapshot with
//                | [] -> 
//                    (EventStore.ClientAPI.StreamPosition.Start, stateBuilder.Zero) 
//                | [x] ->
//                    let state = serializer.DeserializeObj x.Event.Data x.Event.EventType
//                    let jsonValue = JsonValue.Parse <| System.Text.Encoding.UTF8.GetString(x.Event.Metadata)
//                    let lastEventNumber = 
//                        match jsonValue with
//                        | JsonValue.Record [| "lastEventNumber", JsonValue.Number lastEventNumber |] -> Convert.ToInt32(lastEventNumber)
//                        | _ -> failwith <| sprintf "malformed snapshot metadata %s" snapshotStream
//                    (lastEventNumber + 1, state)
//                | _ -> failwith ("unexpected result count when loading snapshot")
//
//            let fold (expectedVersion, unsnapshotted, state) (event : ResolvedEvent) =
//                if types |> Set.contains event.Event.EventType then
//                    let evt = serializer.DeserializeObj event.Event.Data event.Event.EventType
//                    (event.Event.EventNumber, unsnapshotted + 1, stateBuilder.Fold state evt)
//                else
//                    (event.Event.EventNumber, unsnapshotted + 1, state)
//            return! 
//                client.readStreamForward streamId startIndex
//                |> AsyncSeq.fold fold (EventStore.ClientAPI.ExpectedVersion.EmptyStream, 0, zero)
//        }
//        
//    let processMessage streamId (stateBuilder : IStateBuilder<obj,obj>) (handler : obj -> Choice<seq<obj>,_>) =
//         async {
//            let! (expectedVersion, unsnapshotted, state) = getState streamId stateBuilder
//            let result = handler state
//            match result with
//            | Choice1Of2 newEvents ->
//                let eventData =
//                    newEvents
//                    |> Seq.map (fun x -> new EventData(Guid.NewGuid(),  config.TypeToTypeName (x.GetType()), true, serializer.Serialize(x), null))
//                    |> Array.ofSeq
//                do! client.append streamId expectedVersion eventData |> Async.Ignore
//                let eventCount = expectedVersion + eventData.Length + 1
//                if (eventCount > 1 && expectedVersion % 100 = 0) then
//                    let snapshotStream = streamId + "-" + stateBuilder.Name + "-" + stateBuilder.Version
//                    if (unsnapshotted > 100) then
//                        let eventData = new EventData(Guid.NewGuid(), state.GetType().FullName, true, serializer.Serialize state, (System.Text.Encoding.UTF8.GetBytes (sprintf "{ \"lastEventNumber\": %d }" expectedVersion)))
//                        do! client.append snapshotStream EventStore.ClientAPI.ExpectedVersion.Any [|eventData|] |> Async.Ignore 
//                    else
//                        ()
//                else
//                    return ()
//            | _ -> ()
//                
//            return result
//        }
//
//    let processEventList stream messages = async {
//        let rec loop messages' = 
//            match messages' with 
//            | [] -> async { return () }
//            | (_, sb, h : (obj -> seq<obj>))::xs -> async {
//                let h' = h >> Choice1Of2
//                do! processMessage stream sb h' |> Async.Ignore
//                return! loop xs }
//
//        do! loop <| (messages |> List.ofSeq)
//    }
//
//    let processMessages stream messages = async {
//        let rec loop messages' =
//            match messages' with
//            | [] -> async { return () } 
//            | x::xs ->
//                match x with
//                | Event (evt, handlers, _) ->
//                    let handlersForThisStream = handlers |> Map.find stream
//                    processEventList stream handlersForThisStream
//
//        do! loop (messages |> List.ofSeq)
//    }
//
//    let eventComplete (event : Message) = async { 
//        let (Event (_,_,position : EventPosition)) = event
//        completeTracker.Complete position
//    }
//
//    let updatePosition _ = async {
//        let! lastComplete = completeTracker.LastComplete()
//        log <| sprintf "Updating position %A" lastComplete
//        match lastComplete with
//        | Some position ->
//            ProcessingTracker.setPosition client position |> Async.RunSynchronously
//        | None -> () }
//
//    let queue = new WorktrackingQueue<_,_,_>(groupMessageIntoStream, processMessages, 1000, 10, eventComplete)
//
//    let mutable timer : System.Threading.Timer = null
//    let mutable subscription : EventStoreAllCatchUpSubscription = null
//
//    member x.LastComplete =
//        completeTracker.LastComplete
//
//    member x.Start () =  async {
//        let! position = ProcessingTracker.readPosition client |> Async.map (Option.map toGesPosition)
//        let! nullablePosition = match position with
//                                | Some position -> async { return  Nullable(position) }
//                                | None -> 
//                                    log <| "No event position found. Starting from current head."
//                                    async {
//                                        let! nextPosition = client.getNextPosition ()
//                                        return Nullable(nextPosition) }
//
//        let timeBetweenPositionSaves = TimeSpan.FromSeconds(5.0)
//        timer <- new System.Threading.Timer((updatePosition >> Async.RunSynchronously), null, TimeSpan.Zero, timeBetweenPositionSaves)
//        subscription <- client.subscribe position x.EventAppeared (fun () -> ()) }
//
//    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
//        log <| sprintf "Received: %A: %A %A" eventId event.Event.EventType event.OriginalPosition
//
//        async {
//            match config.EventHandlers |> Map.tryFind event.Event.EventType with
//            | Some (t,handlers) ->
//                let position = { Commit = event.OriginalPosition.Value.CommitPosition; Prepare = event.OriginalPosition.Value.PreparePosition }
//                do! completeTracker.Start position
//                let evt = serializer.DeserializeObj (event.Event.Data) t
//                let processList = 
//                    handlers
//                    |> Seq.collect (fun h -> h evt)
//                    |> Seq.toList
//                    |> Seq.groupBy (fun (stream,_,_) -> stream)
//                    |> Map.ofSeq
//
//                do! queue.Add <| Event (evt, processList, position)
//            | None ->
//                let position = event.OriginalPosition.Value |> toEventfulPosition
//                do! completeTracker.Start position
//                completeTracker.Complete position
//        }
//
//    member x.RunCommand cmd streamId =
//        let cmdKey = cmd.GetType().FullName
//        match config.CommandHandlers |> Map.tryFind cmdKey with
//        | Some (t,handler) -> 
//            let (stream, stateBuilder, handler') = handler cmd
//            let state = stateBuilder.Zero
//            processMessage stream stateBuilder handler'
//        | None -> 
//            async { return Choice2Of2 (Seq.singleton <| sprintf "No handler for command: %A" cmdKey) }
//    interface IDisposable with
//        member x.Dispose() =
//            if timer <> null then
//                timer.Dispose()
//            else
//                ()
//
//            if subscription <> null then
//                subscription.Stop(TimeSpan.FromSeconds(30.0))
//            else
//                ()
//            
//            updatePosition () |> Async.RunSynchronously