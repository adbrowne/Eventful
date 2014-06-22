namespace Eventful

open System
open Eventful.EventStream

type EventfulHandler<'T> = EventfulHandler of Type * (obj ->  EventStreamProgram<'T>)

type MyEventResult = unit

type EventfulHandlers
    (
        commandHandlers : Map<string, EventfulHandler<CommandResult>>, 
        eventHandlers : Map<string, EventfulHandler<MyEventResult>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AddCommandHandler = function
        | EventfulHandler(cmdType,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            let commandHandlers' = commandHandlers |> Map.add cmdTypeFullName handler
            new EventfulHandlers(commandHandlers', eventHandlers)
    member x.AddEventHandler (eventType:Type) handler =
        let evtName = eventType.Name
        let eventHandlers' = eventHandlers |> Map.add evtName handler
        new EventfulHandlers(commandHandlers, eventHandlers')

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let empty = new EventfulHandlers(Map.empty, Map.empty)

    let addAggregate (aggregateHandlers : AggregateHandlers<'TState, 'TEvents, 'TId, 'TAggregateType>) (eventfulHandlers:EventfulHandlers) =
        let aggregateTypeString = aggregateHandlers.AggregateType.ToString()

        aggregateHandlers.CommandHandlers
        |> Seq.map (fun x -> EventfulHandler(x.CmdType, x.Handler aggregateTypeString))
        |> Seq.fold (fun (s:EventfulHandlers) h -> s.AddCommandHandler h) eventfulHandlers