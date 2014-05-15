namespace Eventful.Tests.Folding

open Xunit
open System
open FsUnit.Xunit

type StateBuilder<'TState>(zero : 'TState, handlers : List<('TState -> obj -> 'TState)>) = 
    member x.Zero = zero
    member x.AddHandler<'T> (f:'TState -> 'T -> 'TState) =
        let func (state : 'TState) (message : obj) =
            match message with
            | :? 'T as msg -> f state msg
            | _ -> state

        let msgType = typeof<'T>
        new StateBuilder<'TState>(zero, func::handlers)
        
    member x.Run (state: 'TState) (item: obj) =
        handlers
        |> List.fold (fun s h -> h s item) state

    static member Combine<'TState1, 'TState2, 'TState> (builder1 : StateBuilder<'TState1>) (builder2 : StateBuilder<'TState2>) combiner extractor =
       let zero = combiner builder1.Zero builder2.Zero 
       let handler (state : 'TState) (message : obj) = 
            let (state1, state2) = extractor state
            let state1' = builder1.Run state1 message
            let state2' = builder2.Run state2 message
            combiner state1' state2'
       new StateBuilder<'TState>(zero, [handler])
    static member Empty zero = new StateBuilder<'TState>(zero, List.empty)

type ChildAdded = {
    Id : Guid
}

module FoldPlay =
    let runState<'TState> (stateBuilder : StateBuilder<'TState>) items =
        items
        |> List.fold stateBuilder.Run stateBuilder.Zero

    let childCounter = 
        StateBuilder.Empty 0
        |> (fun x -> x.AddHandler (fun x _ -> x + 1))

    [<Fact>]
    let ``Can count children`` () : unit =    
        let result = runState childCounter [{Id = Guid.NewGuid()}]
        result |> should equal 1

    let childIdCollector = 
        StateBuilder.Empty Set.empty
        |> (fun x -> x.AddHandler (fun s { Id = id} -> s |> Set.add id))

    [<Fact>]
    let ``Can collect ids`` () : unit =    
        let stateBuilder = StateBuilder.Combine childCounter childIdCollector (fun count set -> (count,set)) id
        let id = Guid.NewGuid()
        let result = runState stateBuilder [{Id = id}]
        result |> should equal (1, Set.singleton id)