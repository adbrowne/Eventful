namespace Eventful.Tests.Integration

open System
open Xunit
open FsCheck.Xunit
open FsCheck
open Eventful.Raven

module AggregateStatePerisistenceTests = 

    type DateTimeGenerator = 
        static member DateTime() =
            gen {
                let! (DontSize (value : int64)) = Arb.generate
                let value = Math.Abs(value) % DateTime.MaxValue.Ticks //make it in range
                return new DateTime(value)
            }
            |> Arb.fromGen

    [<Property(Arbitrary=[| typeof<DateTimeGenerator> |])>]
    [<Trait("category", "unit")>]
    let ``DateTime can round trip to string with zero padding`` (dateTime : DateTime) = 
        dateTime
        |> Some
        |> AggregateStatePersistence.serializeDateTimeOption 
        |> AggregateStatePersistence.deserializeDateString
        |> ((=) (Some dateTime))

    [<Property(Arbitrary=[| typeof<DateTimeGenerator> |])>]
    [<Trait("category", "unit")>]
    let ``String version of DateTime obeys DateTime ordering`` (dateTime1 : DateTime) (dateTime2 : DateTime) = 
        let stringDateTime1 = AggregateStatePersistence.serializeDateTimeOption (Some dateTime1)
        let stringDateTime2 = AggregateStatePersistence.serializeDateTimeOption (Some dateTime2)
        (stringDateTime1 < stringDateTime2) = (dateTime1 < dateTime2)