namespace Eventful.Tests

open Eventful
open Xunit
open FsCheck
open FsCheck.Xunit

module CacheKeyTests = 
    open Eventful.EventStore.EventStreamInterpreter

    type StreamItem = StreamItem of string * int
    
    type Generators = 
        static member StreamItem() =
            gen {
                let! positiveInt = Arb.Default.Int32().Generator
                                    |> Gen.map abs

                let! streamName =
                    [Gen.constant ':'; Arb.Default.Char().Generator]
                    |> Gen.oneof
                    |> Gen.listOf
                    |> Gen.map (fun chars -> new System.String(List.toArray chars))
                    |> Gen.suchThat (fun s -> s <> "" && not (String.exists ((=) '\000') s))

                return StreamItem(streamName, positiveInt)
            }
            |> Arb.fromGen

    [<Property(Arbitrary=[| typeof<Generators> |])>]
    [<Trait("category", "unit")>]
    let ``CacheKeys Are only equal when they are for the same stream and event number`` (StreamItem (stream1, eventNumber1)) (StreamItem (stream2, eventNumber2)) =
        let inputsAreTheSame = (stream1 = stream2) && (eventNumber1 = eventNumber2)

        let cacheKey1 = getCacheKey stream1 eventNumber1
        let cacheKey2 = getCacheKey stream2 eventNumber2

        let keysAreTheSame = cacheKey1 = cacheKey2

        inputsAreTheSame = keysAreTheSame