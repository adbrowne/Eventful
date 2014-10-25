namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Option
open Eventful.EventStream

type TestSystem<'TMetadata when 'TMetadata : equality>
    (
        handlers : EventfulHandlers<unit,unit,'TMetadata>, 
        lastResult : CommandResult<'TMetadata>, 
        allEvents : TestEventStore<'TMetadata>
    ) =

    let interpret prog (testEventStore : TestEventStore<'TMetadata>) =
        TestInterpreter.interpret 
            prog 
            testEventStore 
            handlers.EventStoreTypeToClassMap 
            handlers.ClassToEventStoreTypeMap
            Map.empty 
            Vector.empty

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

        new TestSystem<'TMetadata>(handlers, result, allEvents)

    member x.Handlers = handlers

    member x.LastResult = lastResult

    member x.Run (cmds : obj list) =
        cmds
        |> List.fold (fun (s:TestSystem<'TMetadata>) cmd -> s.RunCommand cmd) x

    member x.EvaluateState (stream : string) (identity : 'TKey) (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TKey>) =
        let streamEvents = 
            allEvents.Events 
            |> Map.tryFind stream
            |> function
            | Some events -> 
                events
            | None -> Vector.empty

        let run s (evt : obj, metadata) : Map<string,obj> = 
            AggregateStateBuilder.dynamicRun stateBuilder.GetBlockBuilders identity evt metadata s
        
        streamEvents
        |> Vector.map (function
            | (position, Event { Body = body; Metadata = metadata }) ->
                (body, metadata)
            | (position, EventLink (streamId, eventNumber, _)) ->
                allEvents.Events
                |> Map.find streamId
                |> Vector.nth eventNumber
                |> (function
                        | (_, Event { Body = body; Metadata = metadata }) -> (body, metadata)
                        | _ -> failwith ("found link to a link")))
        |> Vector.fold run Map.empty
        |> stateBuilder.GetState

    static member Empty handlers =
        let emptySuccess = {
            Events = List.empty
            Position = None
        }
        new TestSystem<'TMetadata>(handlers, Choice1Of2 emptySuccess, TestEventStore.empty)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x (y:TestSystem<'TMetadata>) = y.RunCommand x