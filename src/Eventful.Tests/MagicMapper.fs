namespace Eventful.Tests

open Xunit
open System
open FsUnit.Xunit
open Eventful
open System.Reflection

module MagicMapperTests =
    type TestRecord = {
        Id : Guid
    }

    [<Fact>]
    let ``can get id from object`` () : unit =
        let id = Guid.NewGuid()
        let myRecord = { Id = id }        
        let result = MagicMapper.magicId<Guid> myRecord
        result |> should equal id

    [<Fact>]
    let ``Will throw where there is no property of the type`` () : unit =
        let id = Guid.NewGuid()
        let myRecord = { Id = id }        
        (fun () -> MagicMapper.magicId<int> myRecord |> ignore) |> should throw typeof<System.Exception>

    type DuplicateIdRecord = {
        Id : Guid
        Id2 : Guid
    }

    [<Fact>]
    let ``Will throw where there are duplicate ids`` () : unit =
        let id = Guid.NewGuid()
        let myRecord = { Id = id; Id2 = id }        
        (fun () -> MagicMapper.magicId<int> myRecord |> ignore) |> should throw typeof<System.Exception>