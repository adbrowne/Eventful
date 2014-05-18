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

    type EventModel (connection : IEventStoreConnection) =
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
            let eventData = new EventData(Guid.NewGuid(), typeof<PersonAddedEvent>.Name, true, serialize cmd, null)
            connection.AppendToStream(streamId, EventStore.ClientAPI.ExpectedVersion.NoStream, eventData)
            ()

    [<Fact>]
    let ``blah`` () : unit =
        let onChildAdded (evt : PersonAddedEvent) =
            match evt.ParentId with
            | Some parentId -> Some (parentId.ToString(), Seq.singleton ({ Id = parentId; ChildId = evt.Id } :> obj))
            | None -> None

        async {
            let! connection = getConnection()

            let model = new EventModel(connection)
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

            model.RunCommand addParentCmd (parentId.ToString())
            model.RunCommand addChildCmd (childId.ToString())

            do! Async.Sleep(1000)

            let! parentStream = connection.ReadStreamEventsBackwardAsync(parentId.ToString(), EventStore.ClientAPI.StreamPosition.End, 1, false) |> Async.AwaitTask

            parentStream.Events
            |> Seq.head
            |> (fun evt -> evt.Event.EventType) |> should equal "ChildAddedEvent"
        } |> Async.RunSynchronously