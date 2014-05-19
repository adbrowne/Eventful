namespace Eventful.Tests.Integration

open Xunit
open EventStore.ClientAPI
open System
open System.IO
open Newtonsoft.Json
open FsUnit.Xunit

module RunningTests = 

    let getConnection () =
        async {
            let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
            let tcs = new System.Threading.Tasks.TaskCompletionSource<unit>()
            let connectionSettingsBuilder = 
                ConnectionSettings
                    .Create()
                    .OnConnected(fun _ _ -> printf "Connected"; )
                    .OnErrorOccurred(fun _ ex -> printfn "Error: %A" ex)
                    .SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)

            return! connection.ConnectAsync().ContinueWith(fun t -> connection) |> Async.AwaitTask
        }

    let log (msg : string) = Console.WriteLine(msg)
        
    let serializer = JsonSerializer.Create()

    let serialize<'T> (t : 'T) =
        use sw = new System.IO.StringWriter() :> System.IO.TextWriter
        serializer.Serialize(sw, t :> obj)
        System.Text.Encoding.UTF8.GetBytes(sw.ToString())

    let deserializeObj (v : byte[]) (objType : Type) : obj =
        let str = System.Text.Encoding.UTF8.GetString(v)
        let reader = new StringReader(str) :> TextReader
        serializer.Deserialize(reader, objType) 

    let deserialize<'T> (v : byte[]) : 'T =
        let objType = typeof<'T>
        (deserializeObj v objType) :?> 'T
        
    type AddPersonCommand = {
        Id : Guid
        Name : string
        ParentId : Guid option
    }

    type PersonAddedEvent = {
        Id : Guid
        Name : string
        ParentId : Guid option
    }

    type ChildAddedEvent = {
        Id : Guid
        ChildId : Guid
    }

    type ChildAddedEvent2 = {
        Id : Guid
        ChildId : Guid
    }

    open FSharpx.Option

    type IStateBuilder =
        inherit IComparable
        abstract member Fold : obj -> obj -> obj
        abstract member Zero : obj

    type cmdHandler = obj -> (string * IStateBuilder * (obj -> Choice<seq<obj>, seq<string>>))

    type EventProcessingConfiguration = {
        CommandHandlers : Map<string, (Type * cmdHandler)>
        StateBuilders: Set<IStateBuilder>
        EventHandlers : Map<string, (Type *  seq<(obj -> seq<(string *  IStateBuilder * (obj -> seq<obj>))>)>)>
    }
    with static member Empty = { CommandHandlers = Map.empty; StateBuilders = Set.empty; EventHandlers = Map.empty } 

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module EventProcessingConfiguration =
        let addCommand<'TCmd, 'TState> (toId : 'TCmd -> string) (stateBuilder : IStateBuilder) (handler : 'TCmd -> 'TState -> Choice<seq<obj>, seq<string>>) (config : EventProcessingConfiguration) = 
            let cmdType = typeof<'TCmd>.FullName
            let outerHandler (cmdObj : obj) =
                let realHandler (cmd : 'TCmd) =
                    let stream = toId cmd
                    let realRealHandler = 
                        let blah = handler cmd
                        fun (state : obj) ->
                            blah (state :?> 'TState)
                    (stream, stateBuilder, realRealHandler)
                match cmdObj with
                | :? 'TCmd as cmd -> realHandler cmd
                | _ -> failwith <| sprintf "Unexpected command type: %A" (cmdObj.GetType())
            config
            |> (fun config -> { config with CommandHandlers = config.CommandHandlers |> Map.add cmdType (typeof<'TCmd>, outerHandler) })
        let addEvent<'TEvt, 'TState> (toId: 'TEvt -> string seq) (stateBuilder : IStateBuilder) (handler : 'TEvt -> 'TState -> seq<obj>) (config : EventProcessingConfiguration) =
            let evtType = typeof<'TEvt>.Name
            let outerHandler (evtObj : obj) : seq<(string * IStateBuilder * (obj -> seq<obj>))> =
                let realHandler (evt : 'TEvt) : seq<(string * IStateBuilder * (obj -> seq<obj>))> =
                    toId evt
                    |> Seq.map (fun stream ->
                        let realRealHandler = 
                            let blah = handler evt
                            fun (state : obj) ->
                                blah (state :?> 'TState)
                        (stream, stateBuilder, realRealHandler))
                match evtObj with
                | :? 'TEvt as evt -> realHandler evt
                | _ -> failwith <| sprintf "Unexpected event type: %A" (evtObj.GetType())
            match config.EventHandlers |> Map.tryFind evtType with
            | Some (_, existing) -> { config with EventHandlers = config.EventHandlers |> Map.add evtType (typeof<'TEvt>, existing |> Seq.append (Seq.singleton outerHandler)) }
            | None ->  { config with EventHandlers = config.EventHandlers |> Map.add evtType (typeof<'TEvt>, Seq.singleton outerHandler) }
            
    type EventModel (connection : IEventStoreConnection, config : EventProcessingConfiguration) =

        member x.Start () = 
            connection.SubscribeToAllAsync(false, (fun subscription event -> x.EventAppeared event event.Event.EventId |> Async.RunSynchronously )) |> Async.AwaitTask

        member x.EventAppeared (event : ResolvedEvent) eventId : Async<unit> =
            log <| sprintf "Received: %A: %A" eventId event.Event.EventType

            async {
                match config.EventHandlers |> Map.tryFind event.Event.EventType with
                | Some (t,handlers) -> 
                    let evt = deserializeObj (event.Event.Data) t
                    do!
                        handlers
                        |> Seq.collect (fun h -> h evt)
                        |> Seq.map (fun (stream, stateBuilder, handler') -> 
                            async {
                                let state = stateBuilder.Zero
                                let result = handler' state
                                let eventData = 
                                    result
                                    |> Seq.map (fun x -> new EventData(Guid.NewGuid(), x.GetType().Name, true, serialize(x), null))
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
                    |> Seq.map (fun e -> new EventData(Guid.NewGuid(), e.GetType().Name, true, serialize e, null)) 
                    |> Seq.toArray

                async {
                    do! connection.AppendToStreamAsync(streamId, EventStore.ClientAPI.ExpectedVersion.NoStream, eventDatas).ContinueWith((fun _ -> true)) |> Async.AwaitTask |> Async.Ignore
                    return result
                }
            | _ -> 
                async {
                    return result
                }

    type MyStateBuilder () =
        interface IStateBuilder with
            member this.Fold state evt = state
            member this.Zero = () :> obj

        interface IComparable with
            member this.CompareTo(obj) = (this.GetType().FullName :> IComparable).CompareTo(obj.GetType().FullName)

    [<Fact>]
    let ``blah`` () : unit =
        let onChildAdded (evt : PersonAddedEvent) (state : unit) =
            match evt.ParentId with
            | Some parentId -> Seq.singleton ({ ChildAddedEvent.Id = parentId; ChildId = evt.Id } :> obj)
            | None -> Seq.empty

        let onChildAdded2 (evt : PersonAddedEvent) (state : unit) =
            match evt.ParentId with
            | Some parentId -> Seq.singleton ({ ChildAddedEvent2.Id = parentId; ChildId = evt.Id } :> obj)
            | None -> Seq.empty

        async {
            let! connection = getConnection()

            let myStateBuilder = new MyStateBuilder() :> IStateBuilder

            let cmdType = typeof<AddPersonCommand>.FullName

            let myCmdHandler (cmd : AddPersonCommand) (state : unit) =
               Choice1Of2 (Seq.singleton ({ PersonAddedEvent.Id = cmd.Id; Name = cmd.Name; ParentId = cmd.ParentId } :> obj)) 

            let evtId (evt : PersonAddedEvent) = 
                match evt.ParentId with
                | Some x -> Seq.singleton (x.ToString())
                | None -> Seq.empty

            let config = 
                EventProcessingConfiguration.Empty
                |> EventProcessingConfiguration.addCommand (fun (cmd : AddPersonCommand) -> cmd.Id.ToString()) myStateBuilder myCmdHandler
                |> EventProcessingConfiguration.addEvent evtId myStateBuilder onChildAdded
                |> EventProcessingConfiguration.addEvent evtId myStateBuilder onChildAdded2

            let model = new EventModel(connection, config)

            do! model.Start() |> Async.Ignore
            
            let parentId = Guid.NewGuid()
            let addParentCmd : AddPersonCommand = {
                Id = parentId
                Name = "Parent"
                ParentId = None
            }

            let childId = Guid.NewGuid()

            let addChildCmd : AddPersonCommand = {
                Id = childId
                Name = "Child"
                ParentId = Some parentId
            }

            let sw = System.Diagnostics.Stopwatch.StartNew()
            do! model.RunCommand addParentCmd (parentId.ToString()) |> Async.Ignore
            Console.WriteLine("First command {0}ms", sw.ElapsedMilliseconds)
            let sw = System.Diagnostics.Stopwatch.StartNew()
            do! model.RunCommand addChildCmd (childId.ToString()) |> Async.Ignore
            Console.WriteLine("Second command {0}ms", sw.ElapsedMilliseconds)

            do! Async.Sleep(1000)

            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! parentStream = connection.ReadStreamEventsBackwardAsync(parentId.ToString(), EventStore.ClientAPI.StreamPosition.End, 100, false) |> Async.AwaitTask

            parentStream.Events
            |> Seq.exists (fun evt -> evt.Event.EventType = "ChildAddedEvent2")
            |> should equal true

            parentStream.Events
            |> Seq.exists (fun evt -> evt.Event.EventType = "ChildAddedEvent")
            |> should equal true

            Console.WriteLine("Read stream command {0}ms", sw.ElapsedMilliseconds)
        } |> Async.RunSynchronously