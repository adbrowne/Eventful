module Eventful.Tests

open Eventful
open Xunit

[<Fact>]
let ``hello returns 42`` () =
  let result = Library.hello 42
  printfn "%i" result
  Assert.Equal(42,result)
