namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx
open FSharpx.Choice
open FSharpx.Option
open Eventful.EventStream

type TestSystemState<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent when 'TMetadata : equality and 'TEventContext :> IDisposable> = {
    Handlers : EventfulHandlers<'TCommandContext,'TEventContext,'TMetadata, 'TBaseEvent> 
    LastResult : CommandResult<'TBaseEvent,'TMetadata>
    AllEvents : TestEventStore<'TMetadata>
    BuildEventContext: PersistedEvent<'TMetadata> -> 'TEventContext
    OnTimeChange : UtcDateTime -> unit
    UseSnapshots : bool
}

type TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent when 'TMetadata : equality and 'TEventContext :> IDisposable>
    (
        state : TestSystemState<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>
    ) =

    let interpret prog (testEventStore : TestEventStore<'TMetadata>) =
        TestInterpreter.interpret 
            prog 
            testEventStore 
            state.UseSnapshots
            state.Handlers.EventStoreTypeToClassMap 
            state.Handlers.ClassToEventStoreTypeMap
            Map.empty 
            Vector.empty

    let interpreter = {
        new IInterpreter<'TMetadata> with
            member x.Run program testEventStore = 
                interpret program testEventStore
    }

    member x.RunStreamProgram program = 
        let (allEvents, result) = interpret program state.AllEvents
        (result, new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>({ state with AllEvents = allEvents }))

    member x.RunCommandNoThrow (cmd : obj) (context : 'TCommandContext) =    
        let program = EventfulHandlers.getCommandProgram context cmd state.Handlers
        let (allEvents, result) = interpret program state.AllEvents

        let allEvents = TestEventStore.processPendingEvents state.BuildEventContext interpreter state.Handlers allEvents

        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>({ state with AllEvents = allEvents; LastResult = result })

    member x.GetStreamMetadata (streamId : string) =
        state.AllEvents.StreamMetadata
        |> Map.tryFind streamId

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
        |> List.fold (fun (s:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) (cmd, context) -> s.RunCommand cmd context) x

    member x.InjectEvent (streamId) (evt : 'TBaseEvent) (metadata : 'TMetadata) =
        let eventType = state.Handlers.ClassToEventStoreTypeMap.Item (evt.GetType())

        let fakeEvent = EventStreamEvent.Event {
            Body = evt :> obj
            EventType = eventType
            Metadata = metadata
        }

        let allEvents' =
            state.AllEvents |> TestEventStore.addEvent streamId fakeEvent
            |> TestEventStore.processPendingEvents state.BuildEventContext interpreter state.Handlers 
        new TestSystem<_,_,_,_>({ state with AllEvents = allEvents' })

    member x.RunToEnd () = 
        let allEvents' = TestEventStore.runToEnd state.OnTimeChange state.BuildEventContext interpreter state.Handlers state.AllEvents 
        let result' = 
            {
                CommandSuccess.Events = List.empty
                Position = None } 
            |> Choice1Of2
        new TestSystem<_,_,_,_>({ state with LastResult = result'; AllEvents = allEvents' })

    member x.Wakeup (wakeupTime : UtcDateTime) (streamId : string) (aggregateType : string) =
        let allEvents' =
            TestEventStore.runWakeup wakeupTime streamId aggregateType interpret state.Handlers state.AllEvents 
        new TestSystem<_,_,_,_>({ state with AllEvents = allEvents' })

    member x.EvaluateState (stream : string) (identity : 'TKey) (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TKey>) =
        let streamEvents = TestEventStore.getAllEvents state.AllEvents stream

        let run s (evt : obj, metadata) : Map<string,obj> = 
            AggregateStateBuilder.dynamicRun stateBuilder.GetBlockBuilders identity evt metadata s
        
        streamEvents
        |> Vector.map (function
            | Event { Body = body; Metadata = metadata } ->
                (body, metadata)
            | EventLink (streamId, eventNumber, _) ->
                match TestEventStore.tryGetEvent state.AllEvents streamId eventNumber with
                | Some (Event { Body = body; Metadata = metadata }) -> (body, metadata)
                | Some (EventLink _) -> failwith "found link to a link"
                | None -> failwith "found a dangling link")
        |> Vector.fold run Map.empty
        |> stateBuilder.GetState

    member x.OnTimeChange onTimeChange =
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>({ state with OnTimeChange = onTimeChange })

    static member EmptyNoSnapshots buildEventContext (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent>) =
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
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>(state)

    static member Empty buildEventContext (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent>) =
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
        new TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>(state)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x c (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) = y.RunCommand x c
    let runCommandNoThrow x c (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) = y.RunCommandNoThrow x c
    let getStreamMetadata streamId (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) = y.GetStreamMetadata streamId
    let runToEnd (y:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) = y.RunToEnd()
    let injectEvent stream event metadata (testSystem : TestSystem<_,_,_,_>) =
        testSystem.InjectEvent stream event metadata
    let wakeup (wakeupTime : UtcDateTime) (streamId : string) (aggregateType : string) (testSystem : TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) =
        testSystem.Wakeup wakeupTime streamId aggregateType 
    let getStreamEvents streamId (system:TestSystem<'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent>) =
        TestEventStore.getAllEvents system.AllEvents streamId
