namespace Eventful

open System
open Eventful.EventStream
open FSharpx.Collections

                                            // Source StreamId, Source Event Number, Event -> Program
type EventfulEventHandler<'T> = EventfulEventHandler of Type * (string -> int -> obj -> EventStreamProgram<'T>)
type EventfulCommandHandler<'T> = EventfulCommandHandler of Type * (obj -> EventStreamProgram<'T>)

type MyEventResult = unit

type EventfulHandlers
    (
        commandHandlers : Map<string, EventfulCommandHandler<CommandResult>>, 
        eventHandlers : Map<string, EventfulEventHandler<MyEventResult> list>,
        eventTypeMap : EventTypeMap
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventTypeMap = eventTypeMap
    member x.AddCommandHandler = function
        | EventfulCommandHandler(cmdType,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            let commandHandlers' = commandHandlers |> Map.add cmdTypeFullName handler
            new EventfulHandlers(commandHandlers', eventHandlers, eventTypeMap)
    member x.AddEventHandler = function
        | EventfulEventHandler(eventType,_) as handler -> 
            let evtName = eventType.Name
            let eventHandlers' = 
                eventHandlers |> Map.insertWith List.append evtName [handler]
            new EventfulHandlers(commandHandlers, eventHandlers', eventTypeMap)
    member x.AddEventMapping (evtType : Type) =
        let shortName = evtType.Name
        let comparableType = new ComparableType(evtType)
        new EventfulHandlers(commandHandlers, eventHandlers, eventTypeMap |> Bimap.addNew shortName comparableType)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let empty = new EventfulHandlers(Map.empty, Map.empty, Bimap.Empty)

    let addCommandHandlers aggregateTypeString (commandHandlers : ICommandHandler<_,_,_> list) eventfulHandlers =
        commandHandlers
        |> Seq.map (fun x -> EventfulCommandHandler(x.CmdType, x.Handler aggregateTypeString))
        |> Seq.fold (fun (s:EventfulHandlers) h -> s.AddCommandHandler h) eventfulHandlers

    let addEventHandlers aggregateTypeString (eventHandlers : IEventHandler<_,_,_> list) eventfulHandlers =
        eventHandlers
        |> Seq.map (fun x -> EventfulEventHandler(x.EventType, x.Handler aggregateTypeString))
        |> Seq.fold (fun (s:EventfulHandlers) h -> s.AddEventHandler h) eventfulHandlers

    let addEventMappings (types : seq<Type>) eventfulHandlers =
        types
        |> Seq.fold (fun (x : EventfulHandlers) y -> x.AddEventMapping y) eventfulHandlers

    let addAggregate (aggregateHandlers : AggregateHandlers<'TState, 'TEvents, 'TId, 'TAggregateType>) (eventfulHandlers:EventfulHandlers) =
        let aggregateTypeString = aggregateHandlers.AggregateType.Name

        eventfulHandlers
        |> addCommandHandlers aggregateTypeString aggregateHandlers.CommandHandlers
        |> addEventHandlers aggregateTypeString aggregateHandlers.EventHandlers
        |> addEventMappings (MagicMapper.getSingleUnionCaseParameterTypes<'TEvents>())

    let getCommandProgram (cmd:obj) (eventfulHandlers:EventfulHandlers) =
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let sourceMessageId = Guid.NewGuid()
        let handler = 
            eventfulHandlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulCommandHandler(_, handler)) -> handler
            | None -> failwith <| sprintf "Could not find handler for %A" cmdType

        handler cmd