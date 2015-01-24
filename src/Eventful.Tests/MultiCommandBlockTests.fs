namespace Eventful.Tests

open Eventful
open Eventful.MultiCommand
open Eventful.Testing
open Xunit
open Swensen.Unquote

module MultiCommandBlockTests = 

    let interpret program = 
        TestMultiCommandInterpreter.interpret program (fun _ _ es -> (es, Choice1Of2 CommandSuccess.Empty)) (TestEventStore.empty) 
        |> ignore
        
    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Catch block is run on exception`` () : unit =
        let exnCaught = ref false
        let program = multiCommand {
            try 
                failwith "Bad"
            with | e ->
                exnCaught := true
        }

        interpret program
        !exnCaught =? true

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Catch block is not run when there is no exception`` () : unit =
        let catchBlockRun = ref false
        let program = multiCommand {
            try 
                return ()
            with | e ->
                catchBlockRun := true
        }

        interpret program

        !catchBlockRun =? false

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Finally block is run on when no exception`` () : unit =
        let finallyRun = ref false
        let program = multiCommand {
            try 
                return ()
            finally
                finallyRun := true
        }

        interpret program

        !finallyRun =? true

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Finally block is run on exception`` () : unit =
        let finallyRun = ref false
        let program = multiCommand {
            try 
                failwith "Bad"
            finally
                finallyRun := true
        }

        try
            interpret program
        with | e ->
            ()

        !finallyRun =? true