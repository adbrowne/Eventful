namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx
open FSharpx.Choice
open FSharpx.Option
open Eventful.EventStream

type TestSystemState<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison and 'TEventContext :> IDisposable> = {
    Handlers : EventfulHandlers<'TCommandContext,'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType> 
    LastResult : CommandResult<'TBaseEvent,'TMetadata>
    AllEvents : TestEventStore<'TMetadata, 'TAggregateType>
    BuildEventContext: PersistedEvent<'TMetadata> -> 'TEventContext
    OnTimeChange : UtcDateTime -> unit
    UseSnapshots : bool
}

type TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison and 'TEventContext :> IDisposable>
    (
        state : TestSystemState<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>
    ) =

    let interpret prog (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        TestInterpreter.interpret 
            prog 
            testEventStore 
            state.UseSnapshots
            state.Handlers.EventStoreTypeToClassMap 
            state.Handlers.ClassToEventStoreTypeMap
            Map.empty 
            Vector.empty

    member x.RunCommandNoThrow (cmd : obj) (context : 'TCommandContext) =    
        let program = EventfulHandlers.getCommandProgram context cmd state.Handlers
        let (allEvents, result) = interpret program state.AllEvents

        let allEvents = TestEventStore.processPendingEvents state.BuildEventContext interpret state.Handlers allEvents

        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>({ state with AllEvents = allEvents; LastResult = result })

    // runs the command. throws on failure
    member x.RunCommand (cmd : obj) (context : 'TCommandContext) =    
        let system = x.RunCommandNoThrow cmd context
        match system.LastResult with
        | Choice1Of2 _ ->
            system
        | Choice2Of2 e ->
            failwith <| sprintf "Command failed %A" e

    member x.Handlers = state.Handlers

    member x.AllEvents = state.AllEvents

    member x.LastResult = state.LastResult

    member x.Run (cmds : (obj * 'TCommandContext) list) =
        cmds
        |> List.fold (fun (s:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) (cmd, context) -> s.RunCommand cmd context) x

    member x.InjectEvent (streamId) (evt : 'TBaseEvent) (metadata : 'TMetadata) =
        let eventType = state.Handlers.ClassToEventStoreTypeMap.Item (evt.GetType())

        let fakeEvent = EventStreamEvent.Event {
            Body = evt :> obj
            EventType = eventType
            Metadata = metadata
        }

        let allEvents' =
            state.AllEvents |> TestEventStore.addEvent streamId fakeEvent
            |> TestEventStore.processPendingEvents state.BuildEventContext interpret state.Handlers 
        new TestSystem<_,_,_,_,_>({ state with AllEvents = allEvents' })

    member x.RunToEnd () = 
        let allEvents' = TestEventStore.runToEnd state.OnTimeChange state.BuildEventContext interpret state.Handlers state.AllEvents 
        let result' = 
            {
                CommandSuccess.Events = List.empty
                Position = None } 
            |> Choice1Of2
        new TestSystem<_,_,_,_,_>({ state with LastResult = result'; AllEvents = allEvents' })

    member x.Wakeup (wakeupTime : UtcDateTime) (streamId : string) (aggregateType : 'TAggregateType) =
        let allEvents' =
            TestEventStore.runWakeup wakeupTime streamId aggregateType interpret state.Handlers state.AllEvents 
        new TestSystem<_,_,_,_,_>({ state with AllEvents = allEvents' })

    member x.EvaluateState (stream : string) (identity : 'TKey) (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TKey>) =
        let streamEvents = 
            state.AllEvents.Events 
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
                state.AllEvents.Events
                |> Map.find streamId
                |> Vector.nth eventNumber
                |> (function
                        | (_, Event { Body = body; Metadata = metadata }) -> (body, metadata)
                        | _ -> failwith ("found link to a link")))
        |> Vector.fold run Map.empty
        |> stateBuilder.GetState

    member x.OnTimeChange onTimeChange =
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>({ state with OnTimeChange = onTimeChange })

    static member EmptyNoSnapshots buildEventContext (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent, 'TAggregateType>) =
        let emptySuccess = {
            CommandSuccess.Events = List.empty
            Position = None
        }
        let state = {
            TestSystemState.Handlers = handlers
            LastResult = Choice1Of2 emptySuccess
            AllEvents = TestEventStore.empty
            BuildEventContext = buildEventContext
            OnTimeChange = (fun _ -> ()) 
            UseSnapshots = false
        }
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>(state)

    static member Empty buildEventContext (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent, 'TAggregateType>) =
        let emptySuccess = {
            CommandSuccess.Events = List.empty
            Position = None
        }
        let state = {
            TestSystemState.Handlers = handlers
            LastResult = Choice1Of2 emptySuccess
            AllEvents = TestEventStore.empty
            BuildEventContext = buildEventContext
            OnTimeChange = (fun _ -> ()) 
            UseSnapshots = true
        }
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>(state)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x c (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) = y.RunCommand x c
    let runCommandNoThrow x c (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) = y.RunCommandNoThrow x c
    let runToEnd (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) = y.RunToEnd()
    let injectEvent stream event metadata (testSystem : TestSystem<_,_,_,_,_>) =
        testSystem.InjectEvent stream event metadata
    let wakeup (wakeupTime : UtcDateTime) (streamId : string) (aggregateType : 'TAggregateType) (testSystem : TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) =
        testSystem.Wakeup wakeupTime streamId aggregateType 
    let getStreamEvents streamId (system:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent, 'TAggregateType>) =
        system.AllEvents.Events
        |> Map.tryFind streamId
        |> Option.getOrElse Vector.empty
        |> Vector.map snd