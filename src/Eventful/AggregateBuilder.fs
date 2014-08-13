namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

open Eventful.EventStream

type CommandResult<'TMetadata> = Choice<list<string * obj * 'TMetadata>,NonEmptyList<ValidationFailure>> 
type StreamNameBuilder<'TId> = ('TId -> string)

type IRegistrationVisitor<'T,'U> =
    abstract member Visit<'TCmd> : 'T -> 'U

type IRegistrationVisitable =
    abstract member Receive<'T,'U> : 'T -> IRegistrationVisitor<'T,'U> -> 'U

type EventResult = unit

type AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateId> = {
    // the combination of all the named state builders
    // for commands and events
    StateBuilder : CombinedStateBuilder

    GetCommandStreamName : 'TCommandContext -> 'TAggregateId -> string
    GetEventStreamName : 'TEventContext -> 'TAggregateId -> string
}

type ICommandHandler<'TEvent,'TId,'TCommandContext, 'TMetadata> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : CombinedStateBuilder -> CombinedStateBuilder
    abstract member GetId : 'TCommandContext-> obj -> 'TId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId> -> 'TCommandContext -> obj -> EventStreamProgram<CommandResult<'TMetadata>,'TMetadata>
    abstract member Visitable : IRegistrationVisitable

type IEventHandler<'TEvent,'TId,'TMetadata> =
    abstract member AddStateBuilder : CombinedStateBuilder -> CombinedStateBuilder
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId> -> 'TEventContext -> string -> int -> EventStreamEventData<'TMetadata> -> EventStreamProgram<EventResult,'TMetadata>

type AggregateCommandHandlers<'TEvents,'TId,'TCommandContext,'TMetadata> = seq<ICommandHandler<'TEvents,'TId,'TCommandContext,'TMetadata>>
type AggregateEventHandlers<'TEvents,'TId,'TMetadata> = seq<IEventHandler<'TEvents,'TId,'TMetadata>>

type AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> private 
    (
        commandHandlers : list<ICommandHandler<'TEvent,'TId,'TCommandContext,'TMetadata>>, 
        eventHandlers : list<IEventHandler<'TEvent,'TId,'TMetadata>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>(handler::commandHandlers, eventHandlers)
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>(commandHandlers, handler::eventHandlers)
    member x.Combine (y:AggregateHandlers<_,_,_,_,_>) =
        new AggregateHandlers<_,_,_,_,_>(
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers)

    static member Empty = new AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>(List.empty, List.empty)
    
type IHandler<'TEvent,'TId,'TCommandContext,'TEventContext> = 
    abstract member add : AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> -> AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>

open FSharpx
open Eventful.Validation

type Validator<'TCmd,'TState> = 
| CommandValidator of ('TCmd -> seq<ValidationFailure>)
| StateValidator of ('TState option -> seq<ValidationFailure>)
| CombinedValidator of ('TCmd -> 'TState option -> seq<ValidationFailure>)

type CommandHandler<'TCmd, 'TCommandContext, 'TCommandState, 'TId, 'TEvent,'TMetadata> = {
    GetId : 'TCommandContext -> 'TCmd -> 'TId
    StateBuilder : NamedStateBuilder<'TCommandState>
    Validators : Validator<'TCmd,'TCommandState> list
    Handler : 'TCommandContext -> 'TCmd -> seq<'TEvent>
    EmtptyMetadata : Guid -> Guid -> 'TMetadata
}

open Eventful.EventStream
open Eventful.Validation

module AggregateActionBuilder =

    let log = createLogger "Eventful.AggregateActionBuilder"

    let simpleHandler<'TId, 'TState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> emptyMetadata stateBuilder (f : 'TCmd -> 'TEvent) =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            Handler = (fun _ -> f >> Seq.singleton)
            EmtptyMetadata = emptyMetadata
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata> 

    let toChoiceValidator cmd r =
        if r |> Seq.isEmpty then
            Success cmd
        else
            NonEmptyList.create (r |> Seq.head) (r |> Seq.tail |> List.ofSeq) |> Failure

    let runValidation validators cmd state =
        let v = new FSharpx.Validation.NonEmptyListValidation<ValidationFailure>()
        validators
        |> List.map (function
                        | CommandValidator validator -> validator cmd |> (toChoiceValidator cmd)
                        | StateValidator validator -> validator state |> (toChoiceValidator cmd)
                        | CombinedValidator validator -> validator cmd state |> (toChoiceValidator cmd))
         |> List.map (fun x -> x)
         |> List.fold (fun s validator -> v.apl validator s) (Choice.returnM cmd) 

    let untypedGetId<'TId,'TCmd,'TEvent,'TState, 'TCommandContext, 'TMetadata> (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata>) (context : 'TCommandContext) (cmd:obj) =
        match cmd with
        | :? 'TCmd as cmd ->
            sb.GetId context cmd
        | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())

    let getChildState combinedState (childStateBuilder : NamedStateBuilder<'TChildState>) stream = eventStream {
            let! (eventsConsumed, state) = 
                combinedState 
                |> CombinedStateBuilder.toStreamProgram stream

            let childState = 
                Option.maybe {
                    let! stateValue = state
                    match stateValue |> Map.tryFind childStateBuilder.Name with
                    | Some hasValue ->
                        return hasValue :?> 'TChildState
                    | None ->
                        return childStateBuilder.Builder.InitialState
                }

            return (eventsConsumed, childState)
        }

    let inline runCommand stream combinedStateBuilder commandStateBuilder buildEmptyMetadata f = 
        let unwrapper = MagicMapper.getUnwrapper<'TEvent>()

        eventStream {
            let! (eventsConsumed, commandState) = getChildState combinedStateBuilder commandStateBuilder stream

            let result = 
                f commandState
                |> Choice.map (fun r ->
                    r
                    |> Seq.map unwrapper
                    |> Seq.map (fun evt -> 
                                    let metadata = buildEmptyMetadata (Guid.NewGuid()) (Guid.NewGuid())
                                    (stream, evt, metadata))
                    |> List.ofSeq)

            return! 
                match result with
                | Choice1Of2 events -> 
                    eventStream {
                        for (stream, event, metadata) in events do
                            let! eventData = getEventStreamEvent event metadata
                            let expectedVersion = 
                                match eventsConsumed with
                                | 0 -> NewStream
                                | x -> AggregateVersion (x - 1)

                            let! writeResult = writeToStream stream expectedVersion (Seq.singleton eventData)

                            log.Debug <| lazy (sprintf "WriteResult: %A" writeResult)
                            
                            ()

                        let lastEvent  = (stream, eventsConsumed + (List.length events))
                        let lastEventNumber = (eventsConsumed + (List.length events) - 1)

                        return Choice1Of2 events
                    }
                | Choice2Of2 x ->
                    eventStream { return Choice2Of2 x }
        }

    let handleCommand 
        (commandHandler:CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata>) 
        (aggregateConfiguration : AggregateConfiguration<_,_,_>) 
        (commandContext : 'TCommandContext) 
        (cmd : obj) =
        let processCommand cmd commandState =
            choose {
                let! validated = runValidation commandHandler.Validators cmd commandState

                let result = commandHandler.Handler commandContext validated
                return result 
            }

        match cmd with
        | :? 'TCmd as cmd -> 
            let id = commandHandler.GetId commandContext cmd
            let stream = aggregateConfiguration.GetCommandStreamName commandContext id
            runCommand stream aggregateConfiguration.StateBuilder commandHandler.StateBuilder commandHandler.EmtptyMetadata (processCommand cmd)
        | _ -> 
            eventStream {
                return NonEmptyList.singleton (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>) |> Choice2Of2
            }
        
    let ToInterface (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata>) = {
        new ICommandHandler<'TEvent,'TId,'TCommandContext,'TMetadata> with 
             member this.GetId context cmd = untypedGetId sb context cmd
             member this.CmdType = typeof<'TCmd>
             member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add sb.StateBuilder
             member this.Handler aggregateConfig commandContext cmd = handleCommand sb aggregateConfig commandContext cmd
             member this.Visitable = {
                new IRegistrationVisitable with
                    member x.Receive a r = r.Visit<'TCmd>(a)
             }
        }

    let buildCmd (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata>) : ICommandHandler<'TEvent,'TId,'TCommandContext,'TMetadata> = 
        ToInterface sb

    let addValidator 
        (validator : Validator<'TCmd,'TState>) 
        (handler: CommandHandler<'TCmd,'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata>) = 
        { handler with Validators = validator::handler.Validators }

    let ensureFirstCommand x = addValidator (StateValidator (isNone id "Must be the first command")) x

    let buildSimpleCmdHandler<'TId,'TCmd,'TCmdState,'TEvent, 'TCommandContext,'TMetadata> emptyMetadata stateBuilder = 
        (simpleHandler<'TId,'TCmdState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> emptyMetadata stateBuilder) >> buildCmd
        
    let getEventInterfaceForLink<'TLinkEvent,'TEvent,'TId,'TMetadata> emptyMetadata (fId : 'TLinkEvent -> 'TId) = {
        new IEventHandler<'TEvent,'TId,'TMetadata> with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder
             member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber (evt : EventStreamEventData<'TMetadata>) = eventStream {
                let metadata = emptyMetadata (System.Guid.NewGuid()) (System.Guid.NewGuid())

                let resultingStream = aggregateConfig.GetEventStreamName eventContext (fId (evt.Body :?> 'TLinkEvent))

                // todo: should not be new stream
                let! _ = EventStream.writeLink resultingStream NewStream sourceStream sourceEventNumber metadata
                return ()
             }
        }

    let getEventInterfaceForOnEvent<'TOnEvent, 'TEvent, 'TId, 'TState, 'TMetadata> (fId : 'TOnEvent -> 'TId) emptyMetadata (stateBuilder : NamedStateBuilder<'TState>) (runEvent : 'TOnEvent -> seq<'TEvent>) = {
        new IEventHandler<'TEvent,'TId,'TMetadata> with 
            member this.EventType = typeof<'TOnEvent>
            member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add stateBuilder
            member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber evt = eventStream {
                let unwrapper = MagicMapper.getUnwrapper<'TEvent>()
                let typedEvent = evt.Body :?> 'TOnEvent
                let resultingStream = aggregateConfig.GetEventStreamName eventContext (fId typedEvent)

                let! (eventsConsumed, state) = getChildState aggregateConfig.StateBuilder stateBuilder resultingStream

                let! eventTypeMap = getEventTypeMap()

                let resultingEvents = 
                    runEvent typedEvent
                    |> Seq.map (fun x -> 
                        let metadata = emptyMetadata (Guid.NewGuid()) (Guid.NewGuid())
                        let event = unwrapper x
                        let eventType = eventTypeMap.FindValue (new ComparableType(event.GetType()))
                        Event { Body = event; EventType = eventType; Metadata = metadata })

                let! _ = EventStream.writeToStream resultingStream NewStream resultingEvents
                return ()
            }
    }

    let linkEvent<'TLinkEvent,'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> emptyMetadata fId (linkEvent : 'TLinkEvent -> 'TEvent) = 
        getEventInterfaceForLink<'TLinkEvent,'TEvent,'TId,'TMetadata> emptyMetadata fId

    let onEvent<'TOnEvent,'TEvent,'TEventState,'TId, 'TMetadata> emptyMetadata fId (stateBuilder : NamedStateBuilder<'TEventState>) (runEvent : 'TOnEvent -> seq<'TEvent>) = 
        getEventInterfaceForOnEvent<'TOnEvent,'TEvent,'TId,'TEventState, 'TMetadata> fId emptyMetadata stateBuilder runEvent

type AggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata> = {
    Configuration : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId>
    Handlers : AggregateHandlers<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
}

module Aggregate = 
    let toAggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
        (getCommandStreamName : 'TCommandContext -> 'TId -> string)
        (getEventStreamName : 'TEventContext -> 'TId -> string) 
        (commandHandlers : AggregateCommandHandlers<'TEvents,'TId,'TCommandContext, 'TMetadata>)
        (eventHandlers : AggregateEventHandlers<'TEvents,'TId,'TMetadata>) = 

            let commandStateBuilders = 
                commandHandlers |> Seq.map (fun x -> x.AddStateBuilder)
            let eventStateBuilders =
                eventHandlers |> Seq.map (fun x -> x.AddStateBuilder)

            let combinedAggregateStateBuilder = 
                commandStateBuilders
                |> Seq.append eventStateBuilders
                |> Seq.fold (|>) CombinedStateBuilder.empty

            let config = {
                StateBuilder = combinedAggregateStateBuilder
                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
            }

            let handlers =
                commandHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddCommandHandler h) AggregateHandlers.Empty

            let handlers =
                eventHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddEventHandler h) handlers

            {
                Configuration = config
                Handlers = handlers
            }