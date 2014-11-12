namespace Eventful

open System
open Eventful.EventStream
open FSharpx.Collections

                                            // Source StreamId, Source Event Number, Event -> Program
type EventfulEventHandler<'T, 'TEventContext, 'TMetadata> = EventfulEventHandler of Type * ('TEventContext -> string -> int -> EventStreamEventData<'TMetadata> -> Async<EventStreamProgram<'T, 'TMetadata>>)
type EventfulCommandHandler<'T, 'TCommandContext, 'TMetadata> = EventfulCommandHandler of Type * ('TCommandContext -> obj -> EventStreamProgram<'T, 'TMetadata>) * IRegistrationVisitable

type MyEventResult = unit

type EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>
    (
        commandHandlers : Map<string, EventfulCommandHandler<CommandResult<'TBaseEvent, 'TMetadata>, 'TCommandContext, 'TMetadata>>, 
        eventHandlers : Map<string, EventfulEventHandler<MyEventResult, 'TEventContext, 'TMetadata> list>,
        wakeup : IWakeupHandler<'TMetadata>,
        eventStoreTypeToClassMap : EventStoreTypeToClassMap,
        classToEventStoreTypeMap : ClassToEventStoreTypeMap
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventStoreTypeToClassMap = eventStoreTypeToClassMap
    member x.ClassToEventStoreTypeMap = classToEventStoreTypeMap
    member x.Wakeup = wakeup

    member x.AddCommandHandler = function
        | EventfulCommandHandler(cmdType,_,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            let commandHandlers' = commandHandlers |> Map.add cmdTypeFullName handler
            new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>(commandHandlers', eventHandlers, wakeup, eventStoreTypeToClassMap, classToEventStoreTypeMap)
    member x.AddEventHandler = function
        | EventfulEventHandler(eventType,_) as handler -> 
            let evtName = eventType.Name
            let eventHandlers' = 
                eventHandlers |> Map.insertWith List.append evtName [handler]
            new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers', wakeup, eventStoreTypeToClassMap, classToEventStoreTypeMap)
    member x.SetWakeUp newWakeup = 
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, newWakeup, eventStoreTypeToClassMap, classToEventStoreTypeMap)
    member x.AddEventStoreTypeToClassMapping (eventStoreType : string) (evtType : Type) =
        let eventStoreTypeToClassMap' = eventStoreTypeToClassMap |> PersistentHashMap.add eventStoreType evtType 
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, wakeup, eventStoreTypeToClassMap', classToEventStoreTypeMap)

    member x.AddClassToEventStoreTypeMap (evtType : Type) (eventStoreType : string) =
        let classToEventStoreTypeMap' = classToEventStoreTypeMap |> PersistentHashMap.add evtType eventStoreType 
        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(commandHandlers, eventHandlers, wakeup, eventStoreTypeToClassMap, classToEventStoreTypeMap')

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let empty<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType> = 
        let noWakeup = {
            new IWakeupHandler<'TMetadata> with
                member x.WakeupFold = Wakeup.noWakeup<'TMetadata>
                member x.Handler = EventStream.empty
        } 

        new EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>(Map.empty, Map.empty, noWakeup, PersistentHashMap.empty, PersistentHashMap.empty)

    let addCommandHandlers config (commandHandlers : ICommandHandler<_,_, _,_> list) eventfulHandlers =
        commandHandlers
        |> Seq.map (fun x -> EventfulCommandHandler(x.CmdType, x.Handler config, x.Visitable))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) h -> s.AddCommandHandler h) eventfulHandlers

    let addEventHandlers config (eventHandlers : IEventHandler<_,_,_> list) eventfulHandlers =
        eventHandlers
        |> Seq.map (fun x -> EventfulEventHandler(x.EventType, x.Handler config))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) h -> s.AddEventHandler h) eventfulHandlers

    let addEventStoreType (eventStoreType : string) (classType : Type) (eventfulHandlers : EventfulHandlers<_,_,_,_,_>) =
        eventfulHandlers.AddEventStoreTypeToClassMapping eventStoreType classType 

    let addClassToEventStoreType (classType : Type) (eventStoreType : string) (eventfulHandlers : EventfulHandlers<_,_,_,_,_>) =
        eventfulHandlers.AddClassToEventStoreTypeMap classType eventStoreType 

    let addAggregate (aggregateDefinition : AggregateDefinition<'TId, 'TCommandContext, 'TEventContext, _,'TBaseEvent,'TAggregateType>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) =
        eventfulHandlers
        |> addCommandHandlers aggregateDefinition.CommandConfiguration aggregateDefinition.Handlers.CommandHandlers
        |> addEventHandlers aggregateDefinition.EventConfiguration aggregateDefinition.Handlers.EventHandlers

    let getCommandProgram (context:'TCommandContext) (cmd:obj) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata,'TBaseEvent,'TAggregateType>) =
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let handler = 
            eventfulHandlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulCommandHandler(_, handler, _)) -> handler
            | None -> failwith <| sprintf "Could not find handler for %A" cmdType

        handler context cmd