namespace Eventful

open System
open FSharpx
open FSharpx.Validation
open FSharpx.Collections

module Validation = 
    let Success = Choice1Of2
    let Failure = Choice2Of2

    let notBlank (f:'A -> string) fieldName (x:'A) : Choice<'A,NonEmptyList<ValidationFailure>> = 
        if f x |> String.IsNullOrWhiteSpace then
            sprintf "%s must not be blank" fieldName |> NonEmptyList.singleton |> Failure 
        else
            Success x