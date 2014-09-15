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

type TestEventMetadata = {
    MessageId: Guid
    SourceMessageId: string
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
    let getStreamName () (testId : TestId) =
        sprintf "Test-%s" <| testId.Id.ToString("N")

    type TestEvents =
    | Test of TestEvent

    let systemConfiguration = { 
        SetSourceMessageId = (fun id metadata -> { metadata with TestEventMetadata.SourceMessageId = id })
        SetMessageId = (fun id metadata -> { metadata with MessageId = id })
    }

    let stateBuilder = NamedStateBuilder.nullStateBuilder<TestEventMetadata>

    let inline simpleHandler f = 
        let withMetadata = f >> (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty }))
        Eventful.AggregateActionBuilder.simpleHandler systemConfiguration stateBuilder withMetadata
    
    let inline buildCmdHandler f =
        f
        |> simpleHandler
        |> buildCmd

    let inline fullHandler s f =
        let withMetadata a b c =
            f a b c
            |> Choice.map (fun evts ->
                evts 
                |> List.map (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty }))
                |> List.toSeq
            )
        Eventful.AggregateActionBuilder.fullHandler systemConfiguration s withMetadata

    let cmdHandlers = 
        seq {
           let testCommand (cmd : TestCommand) =
               Test { 
                   TestId = cmd.TestId
               }

           yield buildCmdHandler testCommand
        }

    let handlers =
        toAggregateDefinition getStreamName getStreamName cmdHandlers Seq.empty

module AggregateConfigurationErrorTests = 

    let emptyTestSystem =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate TestAggregate.handlers
        |> TestSystem.Empty

    [<Fact>]
    let ``Null command id returns error`` () =
        let cmdWithNullId : TestCommand = System.Activator.CreateInstance(typeof<TestCommand>) :?> TestCommand

        let result =
            emptyTestSystem 
            |> TestSystem.runCommand cmdWithNullId

        let matcher (exn : System.Exception) =
            exn.Message = "Object reference not set to an instance of an object."

        result.LastResult |> should containException<TestEventMetadata> (Some "Retrieving aggregate id from command", matcher)