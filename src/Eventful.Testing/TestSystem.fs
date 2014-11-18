namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Option
open Eventful.EventStream

type TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison>
    (
        handlers : EventfulHandlers<'TCommandContext,'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>, 
        lastResult : CommandResult<'TBaseEvent,'TMetadata>, 
        allEvents : TestEventStore<'TMetadata, 'TAggregateType>,
        buildEventContext: 'TMetadata -> 'TEventContext,
        onTimeChange: DateTime -> unit
    ) =

    let interpret prog (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        TestInterpreter.interpret 
            prog 
            testEventStore 
            handlers.EventStoreTypeToClassMap 
            handlers.ClassToEventStoreTypeMap
            Map.empty 
            Vector.empty

    member x.RunCommandNoThrow (cmd : obj) (context : 'TCommandContext) =    
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let handler = 
            handlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulCommandHandler(_, handler,_)) -> handler context
            | None -> failwith <| sprintf "Could not find handler for %A" cmdType

        let (allEvents, result) = TestEventStore.runCommand interpret cmd handler allEvents

        let allEvents = TestEventStore.processPendingEvents buildEventContext interpret handlers allEvents

        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>(handlers, result, allEvents, buildEventContext, onTimeChange)

    // runs the command. throws on failure
    member x.RunCommand (cmd : obj) (context : 'TCommandContext) =    
        let system = x.RunCommandNoThrow cmd context
        match system.LastResult with
        | Choice1Of2 _ ->
            system
        | Choice2Of2 e ->
            failwith <| sprintf "Command failed %A" e

    member x.Handlers = handlers

    member x.LastResult = lastResult

    member x.Run (cmds : (obj * 'TCommandContext) list) =
        cmds
        |> List.fold (fun (s:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) (cmd, context) -> s.RunCommand cmd context) x

    member x.InjectEvent (stream) (eventNumber) (evt : 'TBaseEvent) (metadata : 'TMetadata) =
        let eventType = handlers.ClassToEventStoreTypeMap.Item (evt.GetType())
        let fakeEvent = Event {
            Body = evt :> obj
            Metadata = metadata
            EventType = eventType
        } 
        
        let allEvents' =
            TestEventStore.runEvent buildEventContext interpret handlers allEvents (stream, eventNumber, fakeEvent)
            |> TestEventStore.processPendingEvents buildEventContext interpret handlers 
        new TestSystem<_,_,_,_,_>(handlers, lastResult, allEvents',buildEventContext, onTimeChange)

    member x.RunToEnd () = 
        let allEvents' = TestEventStore.runToEnd onTimeChange buildEventContext interpret handlers allEvents 
        let result' = 
            {
                CommandSuccess.Events = List.empty
                Position = None } 
            |> Choice1Of2
        new TestSystem<_,_,_,_,_>(handlers, result', allEvents',buildEventContext, onTimeChange)

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

    member x.OnTimeChange onTimeChange =
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>(handlers, lastResult, allEvents, buildEventContext, onTimeChange)

    static member Empty (buildEventContext : 'TMetadata -> 'TEventContext) (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent, 'TAggregateType>) =
        let emptySuccess = {
            CommandSuccess.Events = List.empty
            Position = None
        }
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>(handlers, Choice1Of2 emptySuccess, TestEventStore.empty, buildEventContext, (fun _ -> ()))

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x c (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) = y.RunCommand x c
    let runCommandNoThrow x c (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) = y.RunCommandNoThrow x c
    let runToEnd (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) = y.RunToEnd()
    let injectEvent stream eventNumber event metadata (testSystem : TestSystem<_,_,_,_,_>) =
        testSystem.InjectEvent stream eventNumber event metadata