namespace Eventful.Tests

open System
open Eventful
open Eventful.Testing
open FSharpx
open FSharpx.Option

open Xunit
open FsUnit.Xunit

module StateChangeTests =
    open EventSystemTestCommon

    type FooCmd = {
        Id : Guid
        Qty : int
    }

    type FooEvent = {
        Id : Guid
        Qty : int
    }
    with interface IEvent

    type MultipleOf10ReachedEvent = {
        Id : Guid
        Qty : int
    }
    with interface IEvent

    let eventTypes = seq {
        yield typeof<FooEvent>
        yield typeof<MultipleOf10ReachedEvent>
    }

    let fooHandlers =    
        let cmdHandlers = seq {
            yield 
                cmdHandler
                    (fun (cmd : FooCmd) -> 
                        { FooEvent.Id = cmd.Id;  Qty = cmd.Qty } :> IEvent )
                |> AggregateActionBuilder.buildCmd
        }

        let quantityStateBuilder =
            StateBuilder.Empty "Quantity" 0
            |> StateBuilder.handler (fun _ _ -> ()) (fun (qty,e:FooEvent,_) -> qty + e.Qty)

        let emitMultipleOf10Event before after =
            match (after % 10) with
            | 0 -> Seq.singleton ({ MultipleOf10ReachedEvent.Id = Guid.NewGuid(); Qty = after } :> IEvent, metadataBuilder)
            | _ -> Seq.empty

        let quantityStateChangeHandler = 
            AggregateActionBuilder.buildStateChange quantityStateBuilder emitMultipleOf10Event

        let handlers = 
            AggregateHandlers.Empty
            |> AggregateHandlers.addCommandHandlers cmdHandlers
            |> AggregateHandlers.addStateChangeHandler quantityStateChangeHandler 

        Eventful.Aggregate.aggregateDefinitionFromHandlers
            "TestAggregate"
            TestMetadata.GetUniqueId
            getCommandStreamName
            getStreamName
            handlers

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate fooHandlers
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst UnitEventContext) handlers

    let multipleOf10Count : IStateBuilder<int, TestMetadata, unit> =
        StateBuilder.eventTypeCountBuilder (fun (e:MultipleOf10ReachedEvent) _ -> ())
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``When 10 is reached event is emitted`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName UnitEventContext thisId

        let commandId = Guid.NewGuid() 

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { FooCmd.Id = thisId; Qty = 10 } commandId
            |> TestSystem.runToEnd

        let eventCount = afterRun.EvaluateState streamName () multipleOf10Count

        eventCount |> should equal 1

// this is normal fizz buzz as we emit all 3 events when
// they are all divisable
module FizzBuzzStateChangeTests =
    open EventSystemTestCommon

    type IncrimentEvent = IncrimentEvent
    with interface IEvent

    type FizzEvent = FizzEvent
    with interface IEvent

    type BuzzEvent = BuzzEvent
    with interface IEvent

    type FizzBuzzEvent = FizzBuzzEvent
    with interface IEvent

    let eventTypes = seq {
        yield typeof<IncrimentEvent>
        yield typeof<FizzEvent>
        yield typeof<BuzzEvent>
        yield typeof<FizzBuzzEvent>
    }

    type AddTwoNumbersCommand = { Id : Guid }

    let fizzBuzzHandlers =
        let cmdHandlers = seq {
            yield 
                multiEventCmdHandler
                    (fun (_ : AddTwoNumbersCommand) -> seq {
                        yield IncrimentEvent :> IEvent 
                        yield IncrimentEvent :> IEvent 
                    })
                |> AggregateActionBuilder.buildCmd
        }

        let countStateBuilder =
            StateBuilder.Empty "Quantity" 0
            |> StateBuilder.handler (fun _ _ -> ()) (fun (s,e:IncrimentEvent,_) -> s + 1)

        let fizzBuzzStateBuilder =
            StateBuilder.Empty "FizzBuzzStateBuilder" (false, false)
            |> StateBuilder.unitIdHandler (fun (_,e:IncrimentEvent,_) -> (false,false)) // reset on incriment
            |> StateBuilder.unitIdHandler (fun ((_, buzz), e:FizzEvent, _) -> (true, buzz)) // fizz appeared
            |> StateBuilder.unitIdHandler (fun ((fizz, _), e:BuzzEvent, _) -> (fizz, true)) // buzz appeared

        let fizzStateChangeHandler = 
            fun before after ->
                match after % 3 with
                | 0 -> Seq.singleton (FizzEvent :> IEvent, metadataBuilder)
                | _ -> Seq.empty
            |> AggregateActionBuilder.buildStateChange countStateBuilder

        let buzzStateChangeHandler = 
            fun before after ->
                match after % 5 with
                | 0 -> Seq.singleton (BuzzEvent :> IEvent, metadataBuilder)
                | _ -> Seq.empty
            |> AggregateActionBuilder.buildStateChange countStateBuilder

        let fizzBuzzStateChangeHandler = 
            fun before after ->
                match after with
                | (true, true) -> Seq.singleton (FizzBuzzEvent :> IEvent, metadataBuilder)
                | _ -> Seq.empty
            |> AggregateActionBuilder.buildStateChange fizzBuzzStateBuilder

        let handlers = 
            AggregateHandlers.Empty
            |> AggregateHandlers.addCommandHandlers cmdHandlers
            |> AggregateHandlers.addStateChangeHandler fizzStateChangeHandler 
            |> AggregateHandlers.addStateChangeHandler buzzStateChangeHandler 
            |> AggregateHandlers.addStateChangeHandler fizzBuzzStateChangeHandler 

        Eventful.Aggregate.aggregateDefinitionFromHandlers
            "TestAggregate"
            TestMetadata.GetUniqueId
            getCommandStreamName
            getStreamName
            handlers

    let handlers =
        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate fizzBuzzHandlers
        |> StandardConventions.addEventTypes eventTypes

    let emptyTestSystem = TestSystem.Empty (konst UnitEventContext) handlers

    let validateStream : IStateBuilder<(bool * bool * bool * int) option, TestMetadata, unit> =
        StateBuilder.Empty "ValidateStream" (Some (false, false, false, 0))
        |> StateBuilder.unitIdHandler (fun (s, _:IncrimentEvent, _) ->
              maybe {
                  let! (fizzExpected, buzzExpected, fizzBuzzExpected, count) = s
                  return! 
                      match (fizzExpected, buzzExpected, fizzBuzzExpected) with
                      | (false,false, false) -> 
                            let count' = count + 1
                            let fizzExpected' = count' % 3 = 0
                            let buzzExpected' = count' % 5 = 0
                            let fizzBuzzExpected' = fizzExpected' && buzzExpected'
                            Some (fizzExpected', buzzExpected', fizzBuzzExpected', count') // on incriment
                      | _ -> None
              }
        )
        |> StateBuilder.unitIdHandler (fun (s, _:FizzEvent, _) ->
              maybe {
                  let! (fizzExpected, buzzExpected, fizzBuzzExpected, count) = s
                  return! 
                      if fizzExpected then
                        Some (false, buzzExpected, fizzBuzzExpected, count)
                      else 
                        None
              }
        )
        |> StateBuilder.unitIdHandler (fun (s, _:BuzzEvent, _) ->
              maybe {
                  let! (fizzExpected, buzzExpected, fizzBuzzExpected, count) = s
                  return! 
                      if buzzExpected then
                        Some (fizzExpected, false, fizzBuzzExpected, count)
                      else 
                        None
              }
        )
        |> StateBuilder.unitIdHandler (fun (s, _:FizzBuzzEvent, _) ->
              maybe {
                  let! (fizzExpected, buzzExpected, fizzBuzzExpected, count) = s
                  return! 
                      if fizzBuzzExpected then
                        Some (fizzExpected, buzzExpected, false, count)
                      else 
                        None
              }
        )
        |> StateBuilder.toInterface

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Fizz Buzz Emitted`` () : unit =
        let thisId = Guid.NewGuid()
        let streamName = getStreamName UnitEventContext thisId

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())
            |> TestSystem.runCommand { AddTwoNumbersCommand.Id = thisId } (Guid.NewGuid())

        let validateResult = afterRun.EvaluateState streamName () validateStream

        validateResult |> should equal (Some (false, false, false, 20))