namespace Eventful

open System
open Eventful.EventStream
open Eventful.MultiCommand
open FSharpx
open FSharpx.Collections
open FSharp.Control

                                            // Source StreamId, Source Event Number, Event -> Program
type EventfulEventHandler<'T, 'TEventContext, 'TMetadata> = EventfulEventHandler of Type * ('TEventContext -> PersistedEvent<'TMetadata> -> Async<EventStreamProgram<'T, 'TMetadata>>)
type EventfulCommandHandler<'T, 'TCommandContext, 'TMetadata> = EventfulCommandHandler of Type * ('TCommandContext -> obj -> EventStreamProgram<'T, 'TMetadata>) * IRegistrationVisitable
type EventfulMultiCommandEventHandler<'T, 'TEventContext, 'TCommandContext, 'TMetadata, 'TBaseEvent> = EventfulMultiCommandEventHandler of Type * ('TEventContext -> PersistedEvent<'TMetadata> -> MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseEvent,'TMetadata>>)

type EventfulWakeupHandler<'TMetadata> = EventfulWakeupHandler of WakeupFold<'TMetadata> * (string -> UtcDateTime -> EventStreamProgram<EventResult, 'TMetadata>)
type EventfulStreamConfig<'TMetadata> = {
    Wakeup : EventfulWakeupHandler<'TMetadata> option
    StateBuilder : IStateBuilder<Map<string,obj>, 'TMetadata, unit>
    GetUniqueId : 'TMetadata -> Option<string>
}

type EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent> = {
        CommandHandlers : Map<string, EventfulCommandHandler<CommandResult<'TBaseEvent, 'TMetadata>, 'TCommandContext, 'TMetadata>>
        EventHandlers : Map<string, EventfulEventHandler<EventResult, 'TEventContext, 'TMetadata> list>
        MultiCommandEventHandlers : Map<string, EventfulMultiCommandEventHandler<EventResult, 'TEventContext, 'TCommandContext, 'TMetadata, 'TBaseEvent> list>
        AggregateTypes : Map<string,EventfulStreamConfig<'TMetadata>>
        EventStoreTypeToClassMap : EventStoreTypeToClassMap
        ClassToEventStoreTypeMap : ClassToEventStoreTypeMap
        GetCommandCorrelationId : 'TCommandContext -> Guid option
        GetEventCorrelationId : 'TMetadata -> Guid option
        GetAggregateType: 'TMetadata -> string }
with
    member x.AddCommandHandler = function
        | EventfulCommandHandler(cmdType,_,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            { x with CommandHandlers = x.CommandHandlers |> Map.add cmdTypeFullName handler }

    member x.AddEventHandler = function
        | EventfulEventHandler(eventType,_) as handler -> 
            let evtName = eventType.Name
            { x with EventHandlers = x.EventHandlers |> Map.insertWith List.append evtName [handler] }

    member x.AddMultiCommandEventHandler = function
        | EventfulMultiCommandEventHandler(eventType,_) as handler -> 
            let evtName = eventType.Name
            { x with MultiCommandEventHandlers = x.MultiCommandEventHandlers |> Map.insertWith List.append evtName [handler] }

    member x.AddAggregateType aggregateType config  = 
        { x with AggregateTypes = x.AggregateTypes |> Map.add aggregateType config }

    member x.AddEventStoreTypeToClassMapping (eventStoreType : string) (evtType : Type) =
        { x with EventStoreTypeToClassMap = x.EventStoreTypeToClassMap |> PersistentHashMap.add eventStoreType evtType }

    member x.AddClassToEventStoreTypeMap (evtType : Type) (eventStoreType : string) =
        { x with ClassToEventStoreTypeMap = x.ClassToEventStoreTypeMap |> PersistentHashMap.add evtType eventStoreType }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let log = createLogger "Eventful.EventfulHandlers"

    let empty getAggregateType = {
        CommandHandlers = Map.empty
        EventHandlers = Map.empty
        MultiCommandEventHandlers = Map.empty
        AggregateTypes = Map.empty
        EventStoreTypeToClassMap = FSharpx.Collections.PersistentHashMap.empty
        ClassToEventStoreTypeMap = FSharpx.Collections.PersistentHashMap.empty
        GetAggregateType = getAggregateType
        GetCommandCorrelationId = konst None 
        GetEventCorrelationId = konst None }

    let addCommandHandlers config (commandHandlers : ICommandHandler<_,_, _,_> list) eventfulHandlers =
        commandHandlers
        |> Seq.map (fun x -> EventfulCommandHandler(x.CmdType, x.Handler config, x.Visitable))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) h -> s.AddCommandHandler h) eventfulHandlers

    let addEventHandlers config (eventHandlers : IEventHandler<_,_,_,_> list) eventfulHandlers =
        eventHandlers
        |> Seq.map (fun x -> EventfulEventHandler(x.EventType, x.Handler config))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) h -> s.AddEventHandler h) eventfulHandlers

    let addMultiCommandEventHandlers (multiCommandEventHandlers : IMultiCommandEventHandler<_,_,_,_> list) eventfulHandlers =
        multiCommandEventHandlers
        |> Seq.map (fun x -> EventfulMultiCommandEventHandler(x.EventType, x.Handler))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) h -> s.AddMultiCommandEventHandler h) eventfulHandlers

    let addAggregateType aggregateType config (eventfulHandlers : EventfulHandlers<_,_,_,_>) =
        eventfulHandlers.AddAggregateType aggregateType config

    let addEventStoreType (eventStoreType : string) (classType : Type) (eventfulHandlers : EventfulHandlers<_,_,_,_>) =
        eventfulHandlers.AddEventStoreTypeToClassMapping eventStoreType classType 

    let addClassToEventStoreType (classType : Type) (eventStoreType : string) (eventfulHandlers : EventfulHandlers<_,_,_,_>) =
        eventfulHandlers.AddClassToEventStoreTypeMap classType eventStoreType 

    let addAggregate (aggregateDefinition : AggregateDefinition<'TId, 'TCommandContext, 'TEventContext, _,'TBaseEvent>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) =
        let commandStateBuilders = 
            aggregateDefinition.Handlers.CommandHandlers 
            |> List.map (fun x -> x.AddStateBuilder)

        let eventStateBuilders =
            aggregateDefinition.Handlers.EventHandlers 
            |> List.map (fun x -> x.AddStateBuilder)

        let stateChangeStateBuilders =
            aggregateDefinition.Handlers.StateChangeHandlers
            |> List.map (fun x -> x.AddStateBuilder)

        let uniqueIdBuilder = 
            AggregateActionBuilder.uniqueIdBuilder aggregateDefinition.GetUniqueId

        let wakeupBlockBuilders =
            match aggregateDefinition.Wakeup with
            | Some wakeup ->
                (wakeup.WakeupFold.GetBlockBuilders)
            | None -> []

        let combinedAggregateStateBuilder = 
            commandStateBuilders
            |> List.append eventStateBuilders
            |> List.append stateChangeStateBuilders
            |> List.fold (|>) []
            |> List.append uniqueIdBuilder.GetBlockBuilders
            |> List.append aggregateDefinition.ExtraStateBuilders
            |> List.append wakeupBlockBuilders
            |> AggregateStateBuilder.ofStateBuilderList

        let stateChangeHandlers = 
            aggregateDefinition.Handlers.StateChangeHandlers |> LazyList.ofList

        let commandConfig = {
            AggregateConfiguration.StateBuilder = combinedAggregateStateBuilder 
            GetUniqueId = aggregateDefinition.GetUniqueId
            StreamMetadata = aggregateDefinition.StreamMetadata
            GetStreamName = aggregateDefinition.GetCommandStreamName
            StateChangeHandlers = stateChangeHandlers
        }

        let eventConfig = {
            AggregateConfiguration.StateBuilder = combinedAggregateStateBuilder
            GetUniqueId = aggregateDefinition.GetUniqueId
            StreamMetadata = aggregateDefinition.StreamMetadata
            GetStreamName = aggregateDefinition.GetEventStreamName
            StateChangeHandlers = stateChangeHandlers
        }

        let aggregateConfig = {
            EventfulStreamConfig.Wakeup =
                aggregateDefinition.Wakeup
                |> Option.map (fun wakeup ->
                   EventfulWakeupHandler (wakeup.WakeupFold, wakeup.Handler eventConfig)
                ) 
            StateBuilder = combinedAggregateStateBuilder
            GetUniqueId = aggregateDefinition.GetUniqueId
        }

        eventfulHandlers 
        |> addCommandHandlers commandConfig aggregateDefinition.Handlers.CommandHandlers
        |> addEventHandlers eventConfig aggregateDefinition.Handlers.EventHandlers
        |> addAggregateType aggregateDefinition.AggregateType aggregateConfig
        |> addMultiCommandEventHandlers aggregateDefinition.Handlers.MultiCommandEventHandlers

    let getHandlerPrograms (persistedEvent : PersistedEvent<'TMetadata>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) =
        let toProgram (EventfulEventHandler (_, handler)) = 
            (fun context -> handler context persistedEvent)

        eventfulHandlers.EventHandlers
        |> Map.tryFind (persistedEvent.Body.GetType().Name)
        |> Option.map (List.map toProgram)
        |> Option.getOrElse []

    let getMultiCommandEventHandlers (persistedEvent : PersistedEvent<'TMetadata>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) =
        let toProgram (EventfulMultiCommandEventHandler (_, handler)) = 
            (fun context -> handler context persistedEvent)
        
        let result = 
            eventfulHandlers.MultiCommandEventHandlers
            |> Map.tryFind (persistedEvent.Body.GetType().Name)
            |> Option.map (List.map toProgram)
            |> Option.getOrElse []

        result

    let getCommandProgram (context:'TCommandContext) (cmd:obj) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent>) =
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let handler = 
            eventfulHandlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulCommandHandler(_, handler, _)) -> handler
            | None -> 
                let msg = sprintf "Could not find handler for %A" cmdType
                log.Warn <| lazy (msg)
                failwith msg

        handler context cmd