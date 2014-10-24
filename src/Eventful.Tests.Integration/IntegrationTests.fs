namespace Eventful.Tests.Integration

open Eventful

module IntegrationTests =
    let log = createLogger "Eventful.IntegrationTests"

    let runUntilSuccess maxTries f =
        let rec loop attempt =
            try 
                f()
            with | _ ->
                if (attempt < maxTries) then
                    loop (attempt + 1) 
                else
                    reraise()
        loop 0