namespace Eventful.Tests

open System
open Eventful

open Xunit
open FsUnit.Xunit

module CombinedStateBuilderTests = 

    type Event1 = {
        Id : Guid
    }

    let runState (builder : CombinedStateBuilder<unit>) (items : obj list) =
        items
        |> List.map (fun x -> (x,()))
        |> List.fold builder.Run Map.empty

    let countEventsBuilder = 
        StateBuilder.Empty 0
        |> StateBuilder.addHandler (fun s (e: Event1) -> s + 1)
        |> NamedStateBuilder.withName "Count"

    let lastIdBuilder = 
        StateBuilder.Empty None
        |> StateBuilder.addHandler (fun _ (e: Event1) -> Some e.Id)
        |> NamedStateBuilder.withName "Last Id"

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can combine two named state builders with different names`` () =
        let combinedStateBuilder = 
            CombinedStateBuilder.empty
            |> CombinedStateBuilder.add countEventsBuilder
            |> CombinedStateBuilder.add lastIdBuilder

        let id =  Guid.NewGuid()
        let result = runState combinedStateBuilder [{ Id = id }]

        let lastId = CombinedStateBuilder.getValue lastIdBuilder result
        let count = CombinedStateBuilder.getValue countEventsBuilder result

        lastId |> should equal (Some id)
        count |> should equal 1

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given state builder When combined with itself Then returns original statebuilder`` () =
        let single = 
            CombinedStateBuilder.empty
            |> CombinedStateBuilder.add countEventsBuilder

        let combined =
            single
            |> CombinedStateBuilder.add countEventsBuilder

        combined |> should equal single

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given two state builders with same name When combined Then exception thrown`` () =
        let countEventsBuilder2 = 
            StateBuilder.Empty 0
            |> NamedStateBuilder.withName countEventsBuilder.Name

        let combineWithSameName () =
            CombinedStateBuilder.empty
            |> CombinedStateBuilder.add countEventsBuilder
            |> CombinedStateBuilder.add countEventsBuilder2
            |> ignore

        combineWithSameName |> should throw typeof<System.Exception>