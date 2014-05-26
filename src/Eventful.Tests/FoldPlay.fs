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

module FoldPlay =
    let runState<'TState> (stateBuilder : StateBuilder<'TState>) (items : obj list) =
        items
        |> List.fold stateBuilder.Run stateBuilder.Zero

    let childCounter = 
        StateBuilder.Counter<ChildAdded>

    [<Fact>]
    let ``Can count children`` () : unit =    
        let result = runState childCounter [{Id = Guid.NewGuid()}]
        result |> should equal 1

    [<Fact>]
    let ``Counter will ignore items of other types`` () : unit =    
        let result = runState childCounter [{Id = Guid.NewGuid()} :> obj; new obj()]
        result |> should equal 1

    let childIdCollector = 
        StateBuilder.Empty Set.empty
        |> (fun x -> x.AddHandler (fun s { ChildAdded.Id = id} -> s |> Set.add id))

    [<Fact>]
    let ``Can collect ids`` () : unit =    
        let stateBuilder = StateBuilder.Combine childCounter childIdCollector (fun count set -> (count,set)) id
        let id = Guid.NewGuid()
        let result = runState stateBuilder [{Id = id}]
        result |> should equal (1, Set.singleton id)

    [<Fact>]
    let ``Set tracker`` () : unit = 
       let child1Id = Guid.NewGuid()
       let child2Id = Guid.NewGuid()
       let stateBuilder = StateBuilder.SetBuilder (fun (added:ChildAdded) -> added.Id) (fun (removed:ChildRemoved) -> removed.Id)
       let result = runState stateBuilder [{ ChildAdded.Id = child1Id }; {ChildRemoved.Id = child1Id}; {ChildAdded.Id = child2Id}]
       result |> should equal (Set.singleton child2Id)