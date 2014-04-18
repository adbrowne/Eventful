namespace Eventful

[<AutoOpen>]
module Helpers = 
    let inline getOrElse v =
        function
        | Some x -> x
        | None -> v