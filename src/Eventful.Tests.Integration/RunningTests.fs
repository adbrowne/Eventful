namespace Eventful.Tests.Integration

open Xunit
open EventStore.ClientAPI
open System
open System.IO
open Newtonsoft.Json
open FsUnit.Xunit
open Eventful
open Eventful.EventStore

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
        ExistingChildren: int
    }

    type ChildAddedEvent2 = {
        Id : Guid
        ChildId : Guid
        ExistingChildren: int
    }

    open FSharpx.Option
            
    [<Fact>]
    let ``blah`` () : unit =
        let onChildAdded (evt : PersonAddedEvent) (state : int) =
            match evt.ParentId with
            | Some parentId -> Seq.singleton ({ ChildAddedEvent.Id = parentId; ChildId = evt.Id; ExistingChildren = state } :> obj)
            | None -> Seq.empty

        let onChildAdded2 (evt : PersonAddedEvent) (state : int) =
            match evt.ParentId with
            | Some parentId -> Seq.singleton ({ ChildAddedEvent2.Id = parentId; ChildId = evt.Id; ExistingChildren = state } :> obj)
            | None -> Seq.empty

        async {
            let! connection = getConnection()

            let myFold (s : int) (evt : obj) =
                match evt with
                | :? ChildAddedEvent as evt -> s + 1
                | _ -> s

            let myStateBuilder = {
                fold = myFold
                zero = 0
                name = "myStateBuilder"
            }

            let cmdType = typeof<AddPersonCommand>.FullName

            let myCmdHandler (cmd : AddPersonCommand) (state : int) =
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

            let esSerializer() = 
                { new ISerializer with
                    member x.DeserializeObj b t = deserializeObj b t
                    member x.Serialize o = serialize o }

            let finalSlice = connection.ReadAllEventsBackward(Position.End, 1, false)
            let nextPosition = finalSlice.NextPosition
            let model = new EventModel(connection, config, esSerializer())

            model.Start(Some nextPosition) |> ignore
            
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