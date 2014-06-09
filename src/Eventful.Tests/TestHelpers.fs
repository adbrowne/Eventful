namespace Eventful.Testing

open Eventful

module TestHelpers = 
    let (|Equals|_|) expected value =
       if expected = value
       then Some ()
       else None

    let beSuccessWithEvent (x:'A -> bool) = 
        let matches (stream, event : obj, metadata) =
            match event with
            | :? 'A as event ->
                x event
            | _ -> false
        NHamcrest.CustomMatcher<obj>(
            sprintf "Matches %A" x, 
            (fun a ->
                match a with
                | :? Choice<list<string * obj * EventMetadata>,ValidationFailure seq> as result -> 
                    match result with
                    | Choice1Of2 events ->
                        events |> List.exists matches
                    | _ -> false
                | _ -> false)
        )

    let containError x = 
        let matches msg = msg = x
        NHamcrest.CustomMatcher<obj>(
            sprintf "Matches %A" x, 
            (fun a ->
                match a with
                | :? Choice<list<string * obj * EventMetadata>,ValidationFailure seq> as result -> 
                    match result with
                    | Choice2Of2 errors ->
                        errors |> Seq.exists matches
                    | _ -> false
                | _ -> false)
        )