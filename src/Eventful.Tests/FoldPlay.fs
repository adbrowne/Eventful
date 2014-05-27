namespace Eventful.Tests.Folding

open Xunit
open System
open FsUnit.Xunit
open Eventful

type ChildRemoved = {
    Id : Guid
}

type ChildAdded = {
    Id : Guid
}

type StatusMessage = {
    Id : Guid
    Status : string
}

module FoldPlay =
    let runState<'TState> (stateBuilder : StateBuilder<'TState>) (items : obj list) =
        items
        |> List.fold stateBuilder.Run stateBuilder.Zero

    let childCounter = 
        StateBuilder.Counter<ChildAdded>

    [<Fact>]
    let ``Can count children`` () : unit =    
        let result = runState childCounter [{ChildAdded.Id = Guid.NewGuid()}]
        result |> should equal 1

    [<Fact>]
    let ``Counter will ignore items of other types`` () : unit =    
        let result = runState childCounter [{ChildAdded.Id = Guid.NewGuid()} :> obj; new obj()]
        result |> should equal 1

    let childIdCollector = 
        StateBuilder.Empty Set.empty
        |> (fun x -> x.AddHandler (fun s { ChildAdded.Id = id} -> s |> Set.add id))

    [<Fact>]
    let ``Can collect ids`` () : unit =    
        let stateBuilder = StateBuilder.Combine childCounter childIdCollector (fun count set -> (count,set)) id
        let id = Guid.NewGuid()
        let result = runState stateBuilder [{ChildAdded.Id = id}]
        result |> should equal (1, Set.singleton id)

    [<Fact>]
    let ``Set builder`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = StateBuilder.SetBuilder (fun (added:ChildAdded) -> added.Id) (fun (removed:ChildRemoved) -> removed.Id)
       let result = runState stateBuilder [{ ChildAdded.Id = child1Id }; {ChildRemoved.Id = child1Id}; {ChildAdded.Id = child2Id}]
       result |> should equal (Set.singleton child2Id)

    [<Fact>]
    let ``Map builder`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = 
            StateBuilder.Empty (Map.empty : Map<Guid, string>)
            |> StateBuilder.addHandler<Map<Guid,string>,StatusMessage> (StateBuilder.mapMandler (fun x -> x.Id) (fun _ x -> x.Status) "")
       let result = runState stateBuilder [{ StatusMessage.Id = child1Id; Status = "1" }; { StatusMessage.Id = child2Id; Status = "1" } ; { StatusMessage.Id = child2Id; Status = "2" }]

       let expectedMap = 
            Map.empty
            |> Map.add child1Id "1"
            |> Map.add child2Id "2"
       result |> should equal (expectedMap)

    [<Fact>]
    let ``Can combine map handlers`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = 
            StateBuilder.Empty (Map.empty : Map<Guid, string>)
            |> StateBuilder.addHandler (StateBuilder.mapMandler<StatusMessage,_,_> (fun x -> x.Id) (fun _ x -> x.Status) "")
            |> StateBuilder.addHandler (StateBuilder.mapMandler<ChildAdded,_,_> (fun x -> x.Id) (fun s x -> s + "child") "")
       let result = runState stateBuilder [{ StatusMessage.Id = child1Id; Status = "1" }; { StatusMessage.Id = child2Id; Status = "1" } ; { StatusMessage.Id = child2Id; Status = "2" }; { ChildAdded.Id = child2Id }]

       let expectedMap = 
            Map.empty
            |> Map.add child1Id "1"
            |> Map.add child2Id "2child"
       result |> should equal (expectedMap)
       
    [<Fact>]
    let ``Last value`` () : unit =
        let stateBuilder = StateBuilder.lastValue<string,string> id ""

        let result = runState stateBuilder ["first";"second"]

        result |> should equal ("second")

    [<Fact>]
    let ``Can map message types`` () : unit =
        let stateBuilder = 
            StateBuilder.lastValue<Guid,Guid> id Guid.Empty
            |> StateBuilder.mapMessages (fun (x : StatusMessage) -> x.Id)

        let child1Id = Guid.NewGuid()
        let child2Id = Guid.NewGuid()
        let result = runState stateBuilder [{ StatusMessage.Id = child1Id; Status = "ignored" }; { StatusMessage.Id = child2Id; Status = "ignored" }]
        result |> should equal (child2Id)