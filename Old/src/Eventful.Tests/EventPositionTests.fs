namespace Eventful.Tests

open Eventful
open Xunit
open FsCheck
open FsCheck.Xunit

module EventPositionTests = 
    type PositionValues = PositionValues of int64 * int64

    type Generators = 
        static member PositionValues() =
            gen {
                let positiveInt64 = Arb.Default.Int64().Generator
                                    |> Gen.map abs

                let! (prepare : int64) =  positiveInt64
                let! (commit : int64) = 
                    positiveInt64
                    |> Gen.suchThat (fun x -> x >= prepare)
                return PositionValues(commit, prepare)
            }
            |> Arb.fromGen

    [<Property>]
    [<Trait("category", "unit")>]
    let ``Event Position can be round tripped`` (commit : int64) (prepare : int64) : bool =
        let inputPosition = {
            Commit = commit;
            Prepare = prepare
        }

        let token = inputPosition.BuildToken()

        let outputPosition = EventPosition.ParseToken token

        outputPosition = Some inputPosition

    [<Property(Arbitrary=[| typeof<Generators> |])>]
    [<Trait("category", "unit")>]
    let ``Event position token can be string ordered`` (PositionValues (commit1, prepare1)) (PositionValues (commit2, prepare2)) : bool =
        let position1 = new EventStore.ClientAPI.Position(commit1, prepare1)
        let position2 = new EventStore.ClientAPI.Position(commit2, prepare2)

        let eventStoreComparisonResult = (EventStore.ClientAPI.Position.op_LessThan(position1, position2))
        let position1Token = (EventPosition.ofEventStorePosition position1).BuildToken()
        let position2Token = (EventPosition.ofEventStorePosition position2).BuildToken()
        let eventfulTokenComparisonResult  = position1Token < position2Token
        eventStoreComparisonResult = eventfulTokenComparisonResult 