namespace Eventful.Tests

open Xunit
open FsUnit.Xunit
open System
open Eventful.Aggregate
open Eventful.Testing
open Eventful.Testing.TestHelpers

open Eventful
open Eventful.AggregateActionBuilder
open Eventful.Validation

open FSharpx
open FSharpx.Collections

open FSharpx.Choice
open FSharpx.Validation

type TestId = {
    Id : Guid
}

[<CLIMutable>]
type TestCommand = {
    TestId : TestId
}

[<CLIMutable>]
type TestEvent = {
    TestId : TestId
}

module TestAggregate = 
    let getStreamName<'TContext> (_:'TContext) (testId : TestId) =
        sprintf "Test-%s" <| testId.Id.ToString("N")

    type TestEvents =
    | Test of TestEvent

    let stateBuilder = StateBuilder.nullStateBuilder<TestMetadata, unit>

    let inline buildMetadata sourceMessageId = { 
            SourceMessageId = sourceMessageId 
            AggregateType =  "testaggregate" }

    let inline withMetadata f cmd = 
        let cmdResult = f cmd
        (Guid.NewGuid().ToString(), cmdResult, buildMetadata)

    let inline simpleHandler f = 
        Eventful.AggregateActionBuilder.simpleHandler stateBuilder (withMetadata f)
    
    let inline buildCmdHandler f =
        f
        |> simpleHandler
        |> buildCmd

    let inline fullHandler s f =
        let withMetadata a b c =
            f a b c
            |> Choice.map (fun evts ->
                let evts' = 
                    evts 
                    |> List.map (fun x -> (x, buildMetadata))
                    |> List.toSeq
                { 
                    UniqueId = Guid.NewGuid().ToString()
                    Events = evts'
                }
            )
        Eventful.AggregateActionBuilder.fullHandler s withMetadata

    let cmdHandlers = 
        seq {
           let testCommand (cmd : TestCommand) =
               { 
                   TestId = cmd.TestId
               } :> obj

           yield buildCmdHandler testCommand
        }

    let handlers =
        toAggregateDefinition 
            "testaggregate" 
            TestMetadata.GetUniqueId
            getStreamName<_> 
            getStreamName<_>
            cmdHandlers 
            Seq.empty

module AggregateConfigurationErrorTests = 
    open Swensen.Unquote

    let emptyTestSystem =
        EventfulHandlers.empty (konst "testaggregate")
        |> EventfulHandlers.addAggregate TestAggregate.handlers
        |> TestSystem.Empty (konst UnitEventContext)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Null command id returns error`` () =
        let cmdWithNullId : TestCommand = System.Activator.CreateInstance(typeof<TestCommand>) :?> TestCommand

        let result =
            emptyTestSystem 
            |> TestSystem.runCommandNoThrow cmdWithNullId ()

        let matcher (exn : System.Exception) =
            exn.Message = "Object reference not set to an instance of an object."

        result.LastResult |> should containException<TestMetadata> (Some "Retrieving aggregate id from command", matcher)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Command handler for object returns error`` () =
        let cmdHandlers = seq {
           let testCommand (cmd : 'a) = // cmd is generic and will be resolved to obj
               { 
                   TestId = { TestId.Id = Guid.NewGuid() }
               } :> obj

           yield TestAggregate.buildCmdHandler testCommand }

        let buildAggregate () : AggregateDefinition<TestId, Guid, Guid, TestMetadata,obj,string> =
            toAggregateDefinition 
                "testaggregate" 
                TestMetadata.GetUniqueId
                TestAggregate.getStreamName<_> 
                TestAggregate.getStreamName<_>
                cmdHandlers 
                Seq.empty
        raisesWith 
            <@ buildAggregate () @> 
            (fun (e : System.Exception) -> <@ e.Message = "Command handler registered for type object. You might need to specify a type explicitely."@>)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Event handler for object returns error`` () =
        let testId = { TestId.Id = Guid.NewGuid() }
        let evtHandlers = seq {
           yield AggregateActionBuilder.onEvent (fun _ context -> testId) StateBuilder.nullStateBuilder (fun _ _  -> Seq.empty)
        }

        let buildAggregate () : AggregateDefinition<TestId, Guid, Guid, TestMetadata,obj,string> =
            toAggregateDefinition 
                "testaggregate" 
                TestMetadata.GetUniqueId
                TestAggregate.getStreamName<_> 
                TestAggregate.getStreamName<_>
                Seq.empty 
                evtHandlers
        raisesWith 
            <@ buildAggregate () @> 
            (fun (e : System.Exception) -> <@ e.Message = "Event handler registered for type object. You might need to specify a type explicitely."@>)