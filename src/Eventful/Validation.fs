namespace Eventful

open System
open FSharpx
open FSharpx.Validation
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Validation

type ValidationFailure = string option * string

type CommandFailure = 
| CommandException of string option * Exception
| CommandError of string
| CommandFieldError of (string * string)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CommandFailure =
    let ofValidationFailure = function
        | (Some field, error) -> CommandFieldError (field, error)
        | (None, error) -> CommandError error

module Validation = 
    let Success = Choice1Of2
    let Failure = Choice2Of2

    let failWithError error = 
        (None, error)
        |> NonEmptyList.singleton
        |> Failure

    let notBlank (f:'A -> string) fieldName (x:'A) : seq<ValidationFailure> = 
        if f x |> String.IsNullOrWhiteSpace then
            (Some fieldName, sprintf "%s must not be blank" fieldName) |> Seq.singleton
        else
            Seq.empty

    let isNone (f:'A -> 'B option) msg (x:'A) : seq<ValidationFailure> = 
        if f x |> Option.isNone then
            Seq.empty
        else
            msg |> Seq.singleton

    let isFalse (f:'A -> bool) msg (x:'A option) : seq<ValidationFailure> = 
        match x with
        | None ->
           Seq.empty
        | Some x when f x ->
           msg |> Seq.singleton 
        | Some _ ->
            Seq.empty

    let hasText (fieldName:string) (input:string) =
        match String.IsNullOrWhiteSpace input with
        | true ->
            Failure <| NonEmptyList.singleton (Some fieldName, "Must have a value")
        | false ->
            Success input

    let isNumber (fieldName:string) (input:string) =
        let (success, result) = Int32.TryParse(input)                   
        match success with
        | true ->
            Success result
        | false ->
            Failure <| NonEmptyList.singleton (Some fieldName, "Must be a number")

    let hasRange fieldName minValue maxValue input =
        match input with
        | _ when input > maxValue ->
            (Some fieldName, sprintf "Must be less than %d" maxValue)
            |> NonEmptyList.singleton
            |> Failure
        | _ when input < minValue ->
            (Some fieldName, sprintf "Must be greater than %d" minValue)
            |> NonEmptyList.singleton
            |> Failure
        | _ -> Success input

    let hasLength fieldName length (input:string) =
        if input = null then
            (Some fieldName, sprintf "Must have %d characters" length)
            |> NonEmptyList.singleton
            |> Failure
        elif input.Length = length then
            Success input
        else
            (Some fieldName, sprintf "Must have %d characters" length)
            |> NonEmptyList.singleton
            |> Failure
            
    let validatePostcode fieldName postcode =
        hasLength fieldName 4 postcode
        *> isNumber fieldName postcode 
        >>= hasRange fieldName 1 9999