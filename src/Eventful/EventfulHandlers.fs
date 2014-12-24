namespace Eventful

open System
open Eventful.EventStream
open FSharpx
open FSharpx.Collections

                                            // Source StreamId, Source Event Number, Event -> Program
type EventfulEventHandler<'T, 'TEventContext, 'TMetadata> = EventfulEventHandler of Type * ('TEventContext -> PersistedEvent<'TMetadata> -> Async<EventStreamProgram<'T, 'TMetadata>>)
type EventfulCommandHandler<'T, 'TCommandContext, 'TMetadata> = EventfulCommandHandler of Type * ('TCommandContext -> obj -> EventStreamProgram<'T, 'TMetadata>) * IRegistrationVisitable

type EventfulWakeupHandler<'TMetadata> = EventfulWakeupHandler of WakeupFold<'TMetadata> * (string -> UtcDateTime -> EventStreamProgram<EventResult, 'TMetadata>)
type EventfulStreamConfig<'TMetadata> = {
    Wakeup : EventfulWakeupHandler<'TMetadata> option
    StateBuilder : IStateBuilder<Map<string,obj>, 'TMetadata, unit>
    GetUniqueId : 'TMetadata -> Option<string>
}

type EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent,'TAggregateType when 'TAggregateType : comparison>
    (
        commandHandlers : Map<string, EventfulCommandHandler<CommandResult<'TBaseEvent, 'TMetadata>, 'TCommandContext, 'TMetadata>>, 
        eventHandlers : Map<string, EventfulEventHandler<EventResult, 'TEventContext, 'TMetadata> list>,
        aggregateTypes : Map<'TAggregateType,EventfulStreamConfig<'TMetadata>>,
        eventStoreTypeToClassMap : EventStoreTypeToClassMap,
        classToEventStoreTypeMap : ClassToEventStoreTypeMap,
        getAggregateType: 'TMetadata -> 'TAggregateType,
        aggregateTypeStringConversion: (('TAggregateType -> string) * (string -> 'TAggregateType))
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventStoreTypeToClassMap = eventStoreTypeToClassMap
    member x.ClassToEventStoreTypeMap = classToEventStoreTypeMap
    member x.AggregateTypes = aggregateTypes
    member x.GetAggregateType = getAggregateType
    member x.AggregateTypeToString = fst aggregateTypeStringConversion
    member x.StringToAggregateType = snd aggregateTypeStringConversion

    member x.AddCommandHandler = function
        | EventfulCommandHandler(cmdType,_,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            let commandHandlers' = commandHandlers |> Map.add cmdTypeFullName handler
            new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>(commandHandlers', eventHandlers, aggregateTypes, eventStoreTypeToClassMap, classToEventStoreTypeMap, getAggregateType, aggregateTypeStringConversion)

    member x.AddEventHandler = function
        | EventfulEventHandler(eventType,_) as handler -> 
            let evtName = eventType.Name
            let eventHandlers' = 
                eventHandlers |> Map.insertWith List.append evtName [handler]
            new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers', aggregateTypes, eventStoreTypeToClassMap, classToEventStoreTypeMap, getAggregateType, aggregateTypeStringConversion)

    member x.AddAggregateType aggregateType config  = 
        let aggregateTypes' = aggregateTypes |> Map.add aggregateType config
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, aggregateTypes', eventStoreTypeToClassMap, classToEventStoreTypeMap, getAggregateType, aggregateTypeStringConversion)

    member x.AddEventStoreTypeToClassMapping (eventStoreType : string) (evtType : Type) =
        let eventStoreTypeToClassMap' = eventStoreTypeToClassMap |> PersistentHashMap.add eventStoreType evtType 
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, aggregateTypes, eventStoreTypeToClassMap', classToEventStoreTypeMap, getAggregateType, aggregateTypeStringConversion)

    member x.AddClassToEventStoreTypeMap (evtType : Type) (eventStoreType : string) =
        let classToEventStoreTypeMap' = classToEventStoreTypeMap |> PersistentHashMap.add evtType eventStoreType 
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, aggregateTypes, eventStoreTypeToClassMap, classToEventStoreTypeMap', getAggregateType, aggregateTypeStringConversion)

    member x.WithAggregateTypeStringConversion conversionPair =
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, aggregateTypes, eventStoreTypeToClassMap, classToEventStoreTypeMap, getAggregateType, conversionPair)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let log = createLogger "Eventful.EventfulHandlers"

    let private defaultAggregateTypeConversions<'TAggregateType> =
        let toString (a : 'TAggregateType) = a.ToString()
        let fromString (str : string) = Convert.ChangeType(str, typeof<'TAggregateType>) :?> 'TAggregateType

        (toString, fromString)

    let empty getAggregateType = 
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(
            Map.empty, 
            Map.empty, 
            Map.empty, 
            PersistentHashMap.empty, 
            PersistentHashMap.empty,
            getAggregateType,
            defaultAggregateTypeConversions<'TAggregateType>)

    let addCommandHandlers config (commandHandlers : ICommandHandler<_,_, _,_> list) eventfulHandlers =
        commandHandlers
        |> Seq.map (fun x -> EventfulCommandHandler(x.CmdType, x.Handler config, x.Visitable))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) h -> s.AddCommandHandler h) eventfulHandlers

    let addEventHandlers config (eventHandlers : IEventHandler<_,_,_,_> list) eventfulHandlers =
        eventHandlers
        |> Seq.map (fun x -> EventfulEventHandler(x.EventType, x.Handler config))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) h -> s.AddEventHandler h) eventfulHandlers

    let addAggregateType aggregateType config (eventfulHandlers : EventfulHandlers<_,_,_,_,_>) =
        eventfulHandlers.AddAggregateType aggregateType config

    let addEventStoreType (eventStoreType : string) (classType : Type) (eventfulHandlers : EventfulHandlers<_,_,_,_,_>) =
        eventfulHandlers.AddEventStoreTypeToClassMapping eventStoreType classType 

    let addClassToEventStoreType (classType : Type) (eventStoreType : string) (eventfulHandlers : EventfulHandlers<_,_,_,_,_>) =
        eventfulHandlers.AddClassToEventStoreTypeMap classType eventStoreType 

    let withAggregateConversion toString fromString (eventfulHandlers : EventfulHandlers<_,_,_,_,_>) =
        eventfulHandlers.WithAggregateTypeStringConversion (toString,fromString)

    let addAggregate (aggregateDefinition : AggregateDefinition<'TId, 'TCommandContext, 'TEventContext, _,'TBaseEvent,'TAggregateType>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) =
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
            |> List.append wakeupBlockBuilders
            |> AggregateStateBuilder.ofStateBuilderList

        let stateChangeHandlers = 
            aggregateDefinition.Handlers.StateChangeHandlers |> LazyList.ofList

        let commandConfig = {
            AggregateConfiguration.StateBuilder = combinedAggregateStateBuilder 
            GetUniqueId = aggregateDefinition.GetUniqueId
            GetStreamName = aggregateDefinition.GetCommandStreamName
            StateChangeHandlers = stateChangeHandlers
        }

        let eventConfig = {
            AggregateConfiguration.StateBuilder = combinedAggregateStateBuilder
            GetUniqueId = aggregateDefinition.GetUniqueId
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

    let getHandlerPrograms buildEventContext (persistedEvent : PersistedEvent<'TMetadata>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) =
        let toProgram (EventfulEventHandler (_, handler)) = 
            use context = buildEventContext persistedEvent
            handler context persistedEvent

        eventfulHandlers.EventHandlers
        |> Map.tryFind (persistedEvent.Body.GetType().Name)
        |> Option.map (List.map toProgram)
        |> Option.getOrElse []

    let getCommandProgram (context:'TCommandContext) (cmd:obj) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) =
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