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

    let deserialize<'T> (v : byte[]) : 'T =
        let objType = typeof<'T>
        let str = System.Text.Encoding.UTF8.GetString(v)
        let reader = new StringReader(str) :> TextReader
        serializer.Deserialize(reader, typeof<'T>) :?> 'T 
        
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

    open FSharpx.Option

    type IStateBuilder =
        inherit IComparable
        abstract member Fold : obj -> obj -> obj
        abstract member Zero : obj

    type cmdHandler = obj -> (string * IStateBuilder * (obj -> Choice<seq<obj>, seq<string>>))

    type EventProcessingConfiguration = {
        CommandHandlers : Map<string, (Type * cmdHandler)>
        StateBuilders: Set<IStateBuilder>
        EventHandlers : Map<string, (Type *  obj -> (string *  IStateBuilder * unit -> seq<string>))>
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
    type EventModel (connection : IEventStoreConnection, config : EventProcessingConfiguration) =
        let mutable handlers : Map<string, ResolvedEvent -> option<string * seq<obj>>> = Map.empty

        member x.RegisterHandler<'TEvt> (handler : 'TEvt -> option<string * seq<obj>>) =
            let handler' (evtObj : ResolvedEvent) = 
                let esEvent = evtObj
                let evt = deserialize<'TEvt>(esEvent.Event.Data)
                handler evt
            handlers <- handlers |> Map.add typeof<'TEvt>.Name handler' 

        member x.Start () = 
            connection.SubscribeToAllAsync(false, (fun subscription event -> x.EventAppeared event event.Event.EventId)) |> Async.AwaitTask

        member x.EventAppeared event eventId =
            log <| sprintf "Received: %A: %A" eventId event.Event.EventType
            maybe {
                let! handler = handlers |> Map.tryFind (event.Event.EventType)
                let! (stream, events) = handler event
                let eventData = 
                    events
                    |> Seq.map (fun x -> new EventData(Guid.NewGuid(), x.GetType().Name, true, serialize(x), null))
                    |> Array.ofSeq
                let result = connection.AppendToStreamAsync(stream, EventStore.ClientAPI.ExpectedVersion.Any, eventData).ContinueWith((fun _ -> true)) |> Async.AwaitTask |> Async.RunSynchronously
                return result
            } |> ignore

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
        let onChildAdded (evt : PersonAddedEvent) =
            match evt.ParentId with
            | Some parentId -> Some (parentId.ToString(), Seq.singleton ({ Id = parentId; ChildId = evt.Id } :> obj))
            | None -> None

        async {
            let! connection = getConnection()

            let myStateBuilder = new MyStateBuilder() :> IStateBuilder

            let cmdType = typeof<AddPersonCommand>.FullName

            let myCmdHandler (cmd : AddPersonCommand) (state : unit) =
               Choice1Of2 (Seq.singleton ({ PersonAddedEvent.Id = cmd.Id; Name = cmd.Name; ParentId = cmd.ParentId } :> obj)) 

            let config = 
                EventProcessingConfiguration.Empty
                |> EventProcessingConfiguration.addCommand (fun (cmd : AddPersonCommand) -> cmd.Id.ToString()) myStateBuilder myCmdHandler

            let model = new EventModel(connection, config)
            model.RegisterHandler onChildAdded

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

            do! model.RunCommand addParentCmd (parentId.ToString()) |> Async.Ignore
            do! model.RunCommand addChildCmd (childId.ToString()) |> Async.Ignore

            do! Async.Sleep(1000)

            let! parentStream = connection.ReadStreamEventsBackwardAsync(parentId.ToString(), EventStore.ClientAPI.StreamPosition.End, 1, false) |> Async.AwaitTask

            parentStream.Events
            |> Seq.head
            |> (fun evt -> evt.Event.EventType) |> should equal "ChildAddedEvent"
        } |> Async.RunSynchronously