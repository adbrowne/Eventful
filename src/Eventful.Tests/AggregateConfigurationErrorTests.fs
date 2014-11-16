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

    let inline buildMetadata (aggregateId : TestId) messageId sourceMessageId = { 
            SourceMessageId = sourceMessageId 
            MessageId = messageId 
            AggregateId = aggregateId.Id
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
            (fun x -> { TestId.Id = x.AggregateId })
            getStreamName<_> 
            getStreamName<_>
            cmdHandlers 
            Seq.empty

module AggregateConfigurationErrorTests = 

    let emptyTestSystem =
        EventfulHandlers.empty (konst "testaggregate")
        |> EventfulHandlers.addAggregate TestAggregate.handlers
        |> TestSystem.Empty

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