namespace Eventful.Tests

open System
open Xunit
open FsCheck.Xunit
open FsCheck
open Eventful
open Swensen.Unquote

module UtcDateTimeTests = 

    type Generators = 
        static member DateTime() =
            gen {
                let! (DontSize (value : int64)) = Arb.generate
                let value = Math.Abs(value) % DateTime.MaxValue.Ticks //make it in range
                return new DateTime(value, DateTimeKind.Utc)
            }
            |> Arb.fromGen

        static member UtcDateTime() =
            gen {
                let! (DontSize (value : int64)) = Arb.generate
                let value = Math.Abs(value) % DateTime.MaxValue.Ticks //make it in range
                return { UtcDateTime.Ticks = value }
            }
            |> Arb.fromGen

    let roundTripViaString =
        UtcDateTime.fromDateTime
        >> UtcDateTime.toString
        >> UtcDateTime.fromString
        >> UtcDateTime.toDateTime
        
    [<Property(Arbitrary=[| typeof<Generators> |])>]
    [<Trait("category", "unit")>]
    let ``DateTime can round trip to string with zero padding`` (dateTime : DateTime) = 
        dateTime
        |> roundTripViaString
        |> ((=) dateTime)

    [<Property(Arbitrary=[| typeof<Generators> |])>]
    [<Trait("category", "unit")>]
    let ``String version of DateTime obeys DateTime ordering`` (dateTime1 : DateTime) (dateTime2 : DateTime) = 
        let stringDateTime1 = dateTime1 |> UtcDateTime.fromDateTime |> UtcDateTime.toString
        let stringDateTime2 = dateTime2 |> UtcDateTime.fromDateTime |> UtcDateTime.toString
        (stringDateTime1 < stringDateTime2) = (dateTime1 < dateTime2)

    [<Property(Arbitrary=[| typeof<Generators> |])>]
    [<Trait("category", "unit")>]
    let ``UtcDateTime obeys DateTime ordering`` (dateTime1 : DateTime) (dateTime2 : DateTime) = 
        let utcDateTime1 = dateTime1 |> UtcDateTime.fromDateTime
        let utcDateTime2 = dateTime2 |> UtcDateTime.fromDateTime
        (utcDateTime1 < utcDateTime2) = (dateTime1 < dateTime2)

    let unspecifiedDateTime = new DateTime(2007, 1, 1, 0, 0, 0, DateTimeKind.Unspecified)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Unspecified datetime kind raises exception`` () =
        raisesWith 
            <@ UtcDateTime.fromDateTime unspecifiedDateTime @> 
            (fun (e : System.Exception) -> <@ e.Message = "Unknown DateTimeKind: Unspecified"@>)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Local time is round tripped`` () =
        let localDateTime = DateTime.SpecifyKind(unspecifiedDateTime, DateTimeKind.Local)

        localDateTime
        |> roundTripViaString
        |> ((=) localDateTime)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Time coming back from UtcDateTime is Utc`` () =
        let localDateTime = DateTime.SpecifyKind(unspecifiedDateTime, DateTimeKind.Local)

        localDateTime
        |> roundTripViaString
        |> (fun x -> x.Kind)
        =? DateTimeKind.Utc