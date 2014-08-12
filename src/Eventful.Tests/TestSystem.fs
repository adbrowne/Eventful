namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Option
open Eventful.EventStream

type TestSystem
    (
        handlers : EventfulHandlers<unit,unit>, 
        lastResult : CommandResult, 
        allEvents : TestEventStore
    ) =

    let interpret prog (testEventStore : TestEventStore) =
        TestInterpreter.interpret  prog testEventStore handlers.EventTypeMap Map.empty Vector.empty

    member x.RunCommand (cmd : obj) =    
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let sourceMessageId = Guid.NewGuid()
        let handler = 
            handlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulCommandHandler(_, handler,_)) -> handler ()
            | None -> failwith <| sprintf "Could not find handler for %A" cmdType

        let (allEvents, result) = TestEventStore.runCommand interpret cmd handler allEvents

        let allEvents = TestEventStore.processPendingEvents () interpret handlers allEvents

        new TestSystem(handlers, result, allEvents)

    member x.Handlers = handlers

    member x.LastResult = lastResult

    member x.Run (cmds : obj list) =
        cmds
        |> List.fold (fun (s:TestSystem) cmd -> s.RunCommand cmd) x

    member x.EvaluateState<'TState> (stream : string) (stateBuilder : StateBuilder<'TState>) =
        let streamEvents = 
            allEvents.Events 
            |> Map.tryFind stream
            |> function
            | Some events -> 
                events
            | None -> Vector.empty

        streamEvents
        |> Vector.map (function
            | (position, Event { Body = obj }) ->
                obj
            | (position, EventLink (streamId, eventNumber, _)) ->
                allEvents.Events
                |> Map.find streamId
                |> Vector.nth eventNumber
                |> (function
                        | (_, Event { Body = obj }) -> obj
                        | _ -> failwith ("found link to a link")))
        |> Vector.fold stateBuilder.Run stateBuilder.InitialState

    static member Empty handlers =
        new TestSystem(handlers, Choice1Of2 List.empty, TestEventStore.empty)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x (y:TestSystem) = y.RunCommand x