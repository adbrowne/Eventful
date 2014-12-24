namespace Eventful

open System

// DateTime type that is unambiguous
type UtcDateTime = {
    // The value of this property represents the number of 
    // 100-nanosecond intervals that have elapsed since 12:00:00 midnight, January 1, 0001
    Ticks : int64
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module UtcDateTime = 
    /// throws if DateTimeKind is not Local or Utc
    /// DateTime type should aways be specified
    let fromDateTime (dateTime : DateTime) =
        match dateTime.Kind with
        | DateTimeKind.Local ->
            { UtcDateTime.Ticks = dateTime.ToUniversalTime().Ticks }
        | DateTimeKind.Utc ->
            { UtcDateTime.Ticks = dateTime.Ticks }
        | _ -> failwith <| sprintf "Unknown DateTimeKind: %A" dateTime.Kind

    let toDateTime (utcDateTime : UtcDateTime) =
        new DateTime(utcDateTime.Ticks, DateTimeKind.Utc)

    let toString (utcDateTime : UtcDateTime) =
        utcDateTime.Ticks.ToString("D21")

    let fromString (str : string) = 
        str 
        |> System.Int64.Parse
        |> (fun x -> { UtcDateTime.Ticks = x })