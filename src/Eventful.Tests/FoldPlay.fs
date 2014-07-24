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

type AddOwner = {
    Id : Guid
    OwnerId : Guid
}

type RemoveOwner = {
    Id : Guid
    OwnerId : Guid
}

module FoldPlay =
    let runState<'TState> (stateBuilder : StateBuilder<'TState>) (items : obj list) =
        items
        |> List.fold stateBuilder.Run stateBuilder.InitialState

    let childCounter = 
        StateBuilder.Counter<ChildAdded>

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can count children`` () : unit =    
        let result = runState childCounter [{ChildAdded.Id = Guid.NewGuid()}]
        result |> should equal 1

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Types are carried through`` () : unit =    
        childCounter.Types |> should equal [typeof<ChildAdded>]

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Counter will ignore items of other types`` () : unit =    
        let result = runState childCounter [{ChildAdded.Id = Guid.NewGuid()} :> obj; new obj()]
        result |> should equal 1

    let childIdCollector = 
        StateBuilder.Empty Set.empty
        |> (fun x -> x.AddHandler (fun s { ChildAdded.Id = id} -> s |> Set.add id))

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can collect ids`` () : unit =    
        let stateBuilder = StateBuilder.Combine childCounter childIdCollector (fun count set -> (count,set)) id
        let id = Guid.NewGuid()
        let result = runState stateBuilder [{ChildAdded.Id = id}]
        result |> should equal (1, Set.singleton id)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Combine combines types`` () : unit =    
        let stateBuilder = StateBuilder.Combine childCounter childIdCollector (fun count set -> (count,set)) id
        stateBuilder.Types |> should equal [typeof<ChildAdded>]

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Set builder`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = StateBuilder.SetBuilder (fun (added:ChildAdded) -> added.Id) (fun (removed:ChildRemoved) -> removed.Id)
       let result = runState stateBuilder [{ ChildAdded.Id = child1Id }; {ChildRemoved.Id = child1Id}; {ChildAdded.Id = child2Id}]
       result |> should equal (Set.singleton child2Id)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Map builder`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = 
            StateBuilder.Empty (Map.empty : Map<Guid, string>)
            |> StateBuilder.addHandler<Map<Guid,string>,StatusMessage> (StateBuilder.mapHandler (fun x -> x.Id) (fun _ x -> x.Status) "")
       let result = runState stateBuilder [{ StatusMessage.Id = child1Id; Status = "1" }; { StatusMessage.Id = child2Id; Status = "1" } ; { StatusMessage.Id = child2Id; Status = "2" }]

       let expectedMap = 
            Map.empty
            |> Map.add child1Id "1"
            |> Map.add child2Id "2"
       result |> should equal (expectedMap)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can combine map handlers`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = 
            StateBuilder.Empty (Map.empty : Map<Guid, string>)
            |> StateBuilder.addHandler (StateBuilder.mapHandler<StatusMessage,_,_> (fun x -> x.Id) (fun _ x -> x.Status) "")
            |> StateBuilder.addHandler (StateBuilder.mapHandler<ChildAdded,_,_> (fun x -> x.Id) (fun s x -> s + "child") "")
       let result = runState stateBuilder [{ StatusMessage.Id = child1Id; Status = "1" }; { StatusMessage.Id = child2Id; Status = "1" } ; { StatusMessage.Id = child2Id; Status = "2" }; { ChildAdded.Id = child2Id }]

       let expectedMap = 
            Map.empty
            |> Map.add child1Id "1"
            |> Map.add child2Id "2child"
       result |> should equal (expectedMap)
       
    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Last value`` () : unit =
        let stateBuilder = StateBuilder.lastValue<string,string> id ""

        let result = runState stateBuilder ["first";"second"]

        result |> should equal ("second")

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can map message types`` () : unit =
        let stateBuilder = 
            StateBuilder.lastValue<Guid,Guid> id Guid.Empty
            |> StateBuilder.mapMessages (fun (x : StatusMessage) -> x.Id)

        let child1Id = Guid.NewGuid()
        let child2Id = Guid.NewGuid()
        let result = runState stateBuilder [{ StatusMessage.Id = child1Id; Status = "ignored" }; { StatusMessage.Id = child2Id; Status = "ignored" }]
        result |> should equal (child2Id)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Mapping message types returns outer type`` () : unit =
        let stateBuilder = 
            StateBuilder.lastValue<Guid,Guid> id Guid.Empty
            |> StateBuilder.mapMessages (fun (x : StatusMessage) -> x.Id)

        stateBuilder.Types |> should equal [typeof<StatusMessage>]

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Combine set and map`` () : unit =
        let id = Guid.NewGuid()
        let owner1Id = Guid.NewGuid()
        let owner2Id = Guid.NewGuid()
        
        let ownerBuilder = StateBuilder.SetBuilder (fun (added:AddOwner) -> added.OwnerId) (fun (removed:RemoveOwner) -> removed.OwnerId)

        let stateBuilder = 
            StateBuilder.Empty Map.empty
            |> StateBuilder.addHandler (StateBuilder.mapHandler (fun (added:AddOwner) -> added.Id) ownerBuilder.Run Set.empty)
            |> StateBuilder.addHandler (StateBuilder.mapHandler (fun (removed:RemoveOwner) -> removed.Id) ownerBuilder.Run Set.empty)

        let result = runState stateBuilder 
                        [ { AddOwner.Id = id; AddOwner.OwnerId = owner1Id }  // add 1 
                          { AddOwner.Id = id; AddOwner.OwnerId = owner2Id }  // add 2 
                          { RemoveOwner.Id = id; RemoveOwner.OwnerId = owner1Id }  ] // remove 1

        let expectedResult = Map.empty |> Map.add id (Set.singleton owner2Id) // expect only 2 remaining
        result |> should equal expectedResult

        stateBuilder.Types |> List.sortBy (fun x -> x.Name) |> should equal [typeof<AddOwner>; typeof<RemoveOwner>]