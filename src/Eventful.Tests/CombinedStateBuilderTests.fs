namespace Eventful.Tests

open System
open Eventful

open Xunit
open FsUnit.Xunit

module CombinedStateBuilderTests = 

    type Event1 = {
        Id : Guid
    }

    let runState (builder : CombinedStateBuilder) (items : obj list) =
        items
        |> List.fold builder.Run Map.empty

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can combine two named state builders with different names`` () =
        let countEventsBuilder = 
            StateBuilder.Empty 0
            |> StateBuilder.addHandler (fun s (e: Event1) -> s + 1)
            |> NamedStateBuilder.withName "Count"


        let lastIdBuilder = 
            StateBuilder.Empty None
            |> StateBuilder.addHandler (fun _ (e: Event1) -> Some e.Id)
            |> NamedStateBuilder.withName "Last Id"

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