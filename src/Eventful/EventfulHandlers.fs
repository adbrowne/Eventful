namespace Eventful

open System
open Eventful.EventStream
open FSharpx.Collections

                                            // Source StreamId, Source Event Number, Event -> Program
type EventfulEventHandler<'T, 'TEventContext> = EventfulEventHandler of Type * ('TEventContext -> string -> int -> EventStreamEventData -> EventStreamProgram<'T>)
type EventfulCommandHandler<'T, 'TCommandContext> = EventfulCommandHandler of Type * ('TCommandContext -> obj -> EventStreamProgram<'T>)

type MyEventResult = unit

type EventfulHandlers<'TCommandContext, 'TEventContext>
    (
        commandHandlers : Map<string, EventfulCommandHandler<CommandResult, 'TCommandContext>>, 
        eventHandlers : Map<string, EventfulEventHandler<MyEventResult, 'TEventContext> list>,
        eventTypeMap : EventTypeMap
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventTypeMap = eventTypeMap
    member x.AddCommandHandler = function
        | EventfulCommandHandler(cmdType,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            let commandHandlers' = commandHandlers |> Map.add cmdTypeFullName handler
            new EventfulHandlers<'TCommandContext, 'TEventContext>(commandHandlers', eventHandlers, eventTypeMap)
    member x.AddEventHandler = function
        | EventfulEventHandler(eventType,_) as handler -> 
            let evtName = eventType.Name
            let eventHandlers' = 
                eventHandlers |> Map.insertWith List.append evtName [handler]
            new EventfulHandlers<'TCommandContext, 'TEventContext>(commandHandlers, eventHandlers', eventTypeMap)
    member x.AddEventMapping (evtType : Type) =
        let shortName = evtType.Name
        let comparableType = new ComparableType(evtType)
        new EventfulHandlers<'TCommandContext, 'TEventContext>(commandHandlers, eventHandlers, eventTypeMap |> Bimap.addNew shortName comparableType)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let empty<'TCommandContext, 'TEventContext> = new EventfulHandlers<'TCommandContext, 'TEventContext>(Map.empty, Map.empty, Bimap.Empty)

    let addCommandHandlers config (commandHandlers : ICommandHandler<_,_> list) eventfulHandlers =
        commandHandlers
        |> Seq.map (fun x -> EventfulCommandHandler(x.CmdType, x.Handler config))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext>) h -> s.AddCommandHandler h) eventfulHandlers

    let addEventHandlers config (eventHandlers : IEventHandler<_,_> list) eventfulHandlers =
        eventHandlers
        |> Seq.map (fun x -> EventfulEventHandler(x.EventType, x.Handler config))
        |> Seq.fold (fun (s:EventfulHandlers<'TCommandContext, 'TEventContext>) h -> s.AddEventHandler h) eventfulHandlers

    let addEventMappings (types : seq<Type>) eventfulHandlers =
        types
        |> Seq.fold (fun (x : EventfulHandlers<'TCommandContext, 'TEventContext>) y -> x.AddEventMapping y) eventfulHandlers

    let addAggregate (aggregateDefinition : AggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext>) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext>) =
        let config = aggregateDefinition.Configuration

        eventfulHandlers
        |> addCommandHandlers config aggregateDefinition.Handlers.CommandHandlers
        |> addEventHandlers config aggregateDefinition.Handlers.EventHandlers
        |> addEventMappings (MagicMapper.getSingleUnionCaseParameterTypes<'TEvents>())

    let getCommandProgram (context:'TCommandContext) (cmd:obj) (eventfulHandlers:EventfulHandlers<'TCommandContext, 'TEventContext>) =
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let sourceMessageId = Guid.NewGuid()
        let handler = 
            eventfulHandlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulCommandHandler(_, handler)) -> handler
            | None -> failwith <| sprintf "Could not find handler for %A" cmdType

        handler context cmd