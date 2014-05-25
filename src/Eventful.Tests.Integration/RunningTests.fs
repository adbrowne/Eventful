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

    let getConnection () : Async<IEventStoreConnection> =
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

    let deserializeObj (v : byte[]) (typeName : string) : obj =
        let objType = Type.GetType typeName
        let str = System.Text.Encoding.UTF8.GetString(v)
        let reader = new StringReader(str) :> TextReader
        let result = serializer.Deserialize(reader, objType) 
        result
        
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

    let getNextPosition ( connection : IEventStoreConnection ) =
        let position = Position.End
        let finalSlice = connection.ReadAllEventsBackward(position, 1, false, null)
        let nextPosition = finalSlice.NextPosition
        nextPosition

    open FSharpx.Option
    open FSharp.Control
       
    let onChildAdded (evt : PersonAddedEvent) (state : int) =
        match evt.ParentId with
        | Some parentId -> Seq.singleton ({ ChildAddedEvent.Id = parentId; ChildId = evt.Id; ExistingChildren = state } :> obj)
        | None -> Seq.empty

    let onChildAdded2 (evt : ChildAddedEvent) (state : int) =
        Seq.singleton ({ ChildAddedEvent2.Id = evt.Id; ChildId = evt.Id; ExistingChildren = state } :> obj)

    [<Fact>]
    let ``Basic commands and events`` () : unit =
        let matches id existingChildren expectedParentId = 
            let idMatches = id = expectedParentId 
            let childrenMatches = existingChildren = 1
            idMatches && childrenMatches

        async {
            let! connection = getConnection()

            let myFold (s : int) (evt : obj) = s + 1

            let myStateBuilder = {
                fold = myFold
                zero = 0
                name = "myStateBuilder"
                version = "1"
                types = Seq.singleton typeof<ChildAddedEvent>
            }

            let cmdType = typeof<AddPersonCommand>.FullName

            let myCmdHandler (cmd : AddPersonCommand) (state : int) =
               Choice1Of2 (Seq.singleton ({ PersonAddedEvent.Id = cmd.Id; Name = cmd.Name; ParentId = cmd.ParentId } :> obj)) 

            let evtId (evt : PersonAddedEvent) = 
                match evt.ParentId with
                | Some x -> Seq.singleton (x.ToString())
                | None -> Seq.empty

            let evtId (evt : PersonAddedEvent) = 
                match evt.ParentId with
                | Some x -> Seq.singleton (x.ToString())
                | None -> Seq.empty
            
            let evtId2 (evt : ChildAddedEvent) =
                Seq.singleton (evt.Id.ToString())

            let config = 
                EventProcessingConfiguration.Empty
                |> EventProcessingConfiguration.addCommand (fun (cmd : AddPersonCommand) -> cmd.Id.ToString()) myStateBuilder myCmdHandler
                |> EventProcessingConfiguration.addEvent evtId myStateBuilder onChildAdded
                |> EventProcessingConfiguration.addEvent evtId2 myStateBuilder onChildAdded2

            let esSerializer = 
                { new ISerializer with
                    member x.DeserializeObj b t = deserializeObj b t
                    member x.Serialize o = serialize o }
            
            use model = new EventModel(connection, config, esSerializer)

            let nextPosition = getNextPosition connection
            do! model.Start(Some nextPosition) |> Async.Ignore
            
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

            do! Async.Sleep(10000)

            let sw = System.Diagnostics.Stopwatch.StartNew()
            let client = new Client(connection)

            let parentStream = client.readStreamBackward <| parentId.ToString() |> Seq.ofAsyncSeq |> List.ofSeq

            printfn "Event count: %d" parentStream.Length
            let firstPart = 
                parentStream
                |> Seq.where (fun evt -> evt.Event.EventType = typeof<ChildAddedEvent2>.FullName)
                |> Seq.map (fun evt -> esSerializer.DeserializeObj evt.Event.Data typeof<ChildAddedEvent2>.FullName :?> ChildAddedEvent2)
                |> Seq.toList

            firstPart
            |> Seq.map (fun evt ->  
                match evt with
                | { ChildAddedEvent2.Id = id; ExistingChildren = existingChildren } -> matches id existingChildren parentId)
            |> Seq.toList
            |> should equal [true]

            parentStream
            |> Seq.exists (fun evt -> evt.Event.EventType = typeof<ChildAddedEvent>.FullName)
            |> should equal true

            Console.WriteLine("Read stream command {0}ms", sw.ElapsedMilliseconds)
            log <| sprintf "Last Complete: %A" (model.LastComplete())
        } |> Async.RunSynchronously

    type AddedEvent = {
        Id : Guid
        Count : int
    }

    [<Fact>]
    let ``Snapshotting`` () : unit =
        async {
            let! connection = getConnection()

            let myFold (s : int) (evt : obj) = s + 1

            let myStateBuilder = {
                fold = myFold
                zero = 1
                name = "myStateBuilder"
                version = "1"
                types = Seq.singleton typeof<AddedEvent>
            }

            let cmdType = typeof<AddPersonCommand>.FullName

            let myCmdHandler (cmd : AddPersonCommand) (state : int) =
               Choice1Of2 (Seq.singleton ({ AddedEvent.Id = cmd.Id; Count = state } :> obj)) 

            let config = 
                EventProcessingConfiguration.Empty
                |> EventProcessingConfiguration.addCommand (fun (cmd : AddPersonCommand) -> cmd.Id.ToString()) myStateBuilder myCmdHandler

            let esSerializer = 
                { new ISerializer with
                    member x.DeserializeObj b t = deserializeObj b t
                    member x.Serialize o = serialize o }
            
            use model = new EventModel(connection, config, esSerializer)

            let nextPosition = getNextPosition connection
            do! model.Start(Some nextPosition) |> Async.Ignore
            
            let parentId = Guid.NewGuid()

            let childId = Guid.NewGuid()

            let addChildCmd : AddPersonCommand = {
                Id = childId
                Name = "Child"
                ParentId = Some parentId
            }

            let sw = System.Diagnostics.Stopwatch.StartNew()

            [1..1000]
            |> Seq.iter (fun _ -> model.RunCommand addChildCmd (childId.ToString()) |> Async.Ignore |> Async.RunSynchronously)
                
            Console.WriteLine("Second command {0}ms", sw.ElapsedMilliseconds)

            let sw = System.Diagnostics.Stopwatch.StartNew()
            let client = new Client(connection)

            let last = client.readStreamBackward <| childId.ToString() |> AsyncSeq.take 1 |> Seq.ofAsyncSeq |> Seq.head

            let lastEvent = deserializeObj last.Event.Data last.Event.EventType :?> AddedEvent

            lastEvent.Count |> should equal 1000

            log <| sprintf "Last Complete: %A" (model.LastComplete())
        } |> Async.RunSynchronously