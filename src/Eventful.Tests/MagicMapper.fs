namespace Eventful.Tests

open Xunit
open System
open FsUnit.Xunit
open Eventful
open System.Reflection

module MagicMapperTests =
    type FooRecord = {
        Id : Guid
    }

    type BarRecord = {
        BarId : Guid
    }

    type UnionType = 
        | Foo of FooRecord
        | Bar of BarRecord

    [<Fact>]
    let ``can get id from object`` () : unit =
        let id = Guid.NewGuid()
        let myRecord = { Id = id }        
        let result = MagicMapper.magicId<Guid> myRecord
        result |> should equal id

    [<Fact>]
    let ``can precompute id getter`` () : unit =
        let id = Guid.NewGuid()
        let myRecord = { Id = id }        
        let getter = MagicMapper.magicIdFromType<Guid> (typeof<FooRecord>)
        let result = getter myRecord
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

    [<Fact>]
    let ``Can wrap object in discriminated union`` () : unit =
        let id = Guid.NewGuid()
        let fooRecord = { FooRecord.Id = id; }        
        let wrapper = MagicMapper.getWrapper<UnionType>()

        let result = wrapper fooRecord
        result |> should equal (Foo fooRecord)

    [<Fact>]
    let ``Will throw when there is no matching union case`` () : unit =
        let id = Guid.NewGuid()
        let myRecord = { Id = id; Id2 = id }        
        let wrapper = MagicMapper.getWrapper<UnionType>()

        (fun () -> wrapper myRecord |> ignore) |> should throw typeof<System.Exception>

    [<Fact>]
    let ``Can unwrap union value`` () : unit =
        let id = Guid.NewGuid()
        let fooRecord = { FooRecord.Id = id; }        

        let wrapped = (Foo fooRecord)
        let unwrapper = MagicMapper.getUnwrapper<UnionType>()

        let result = unwrapper wrapped

        result |> should equal fooRecord