namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

open Eventful.EventStream

type CommandResult = Choice<list<string * obj * EventMetadata>,NonEmptyList<ValidationFailure>> 
type StreamNameBuilder<'TId> = ('TId -> string)

type EventResult = unit

type AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateId> = {
    // the combination of all the named state builders
    // for commands and events
    StateBuilder : CombinedStateBuilder

    GetCommandStreamName : 'TAggregateId -> string
    GetEventStreamName : 'TAggregateId -> string
}

type ICommandHandler<'TEvent,'TId when 'TId :> IIdentity> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : CombinedStateBuilder -> CombinedStateBuilder
    abstract member GetId : obj -> 'TId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId> -> obj -> EventStreamProgram<CommandResult>

type IEventHandler<'TEvent,'TId> =
    abstract member AddStateBuilder : CombinedStateBuilder -> CombinedStateBuilder
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId> -> string -> int -> EventStreamEventData -> EventStreamProgram<EventResult>

type AggregateHandlers<'TEvent,'TId when 'TId :> IIdentity> private 
    (
        commandHandlers : list<ICommandHandler<'TEvent,'TId>>, 
        eventHandlers : list<IEventHandler<'TEvent,'TId>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TEvent,'TId>(handler::commandHandlers, eventHandlers)
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TEvent,'TId>(commandHandlers, handler::eventHandlers)
    member x.Combine (y:AggregateHandlers<_,_>) =
        new AggregateHandlers<_,_>(
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers)

    static member Empty = new AggregateHandlers<'TEvent,'TId>(List.empty, List.empty)
    
type IHandler<'TEvent,'TId when 'TId :> IIdentity> = 
    abstract member add : AggregateHandlers<'TEvent,'TId> -> AggregateHandlers<'TEvent,'TId>

open FSharpx
open Eventful.Validation

type Validator<'TCmd,'TState> = 
| CommandValidator of ('TCmd -> seq<ValidationFailure>)
| StateValidator of ('TState option -> seq<ValidationFailure>)
| CombinedValidator of ('TCmd -> 'TState option -> seq<ValidationFailure>)

type CommandHandler<'TCmd, 'TCommandState, 'TId, 'TEvent when 'TId :> IIdentity> = {
    GetId : 'TCmd -> 'TId
    StateBuilder : NamedStateBuilder<'TCommandState>
    Validators : Validator<'TCmd,'TCommandState> list
    Handler : 'TCmd -> seq<'TEvent>
}

open Eventful.EventStream
open Eventful.Validation

module AggregateActionBuilder =

    let log = createLogger "Eventful.AggregateActionBuilder"

    let simpleHandler<'TId, 'TState,'TCmd,'TEvent when 'TId :> IIdentity> stateBuilder (f : 'TCmd -> 'TEvent) =
        {
            GetId = MagicMapper.magicId<'TId>
            StateBuilder = stateBuilder
            Validators = List.empty
            Handler = f >> Seq.singleton
        } : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent> 

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

    let untypedGetId<'TId,'TCmd,'TEvent,'TState when 'TId :> IIdentity> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) (cmd:obj) =
        match cmd with
        | :? 'TCmd as cmd ->
            sb.GetId cmd
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

    let handleCommand (commandHandler:CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) (aggregateConfiguration : AggregateConfiguration<_,_,_>) (cmd : obj) =
        let unwrapper = MagicMapper.getUnwrapper<'TEvent>()
        eventStream {
            match cmd with
            | :? 'TCmd as cmd -> 
                let id = commandHandler.GetId cmd
                let stream = aggregateConfiguration.GetCommandStreamName id
                
                let! (eventsConsumed, commandState) = getChildState aggregateConfiguration.StateBuilder commandHandler.StateBuilder stream

                let result = choose {
                    let! validated = runValidation commandHandler.Validators cmd commandState

                    let result = commandHandler.Handler validated
                    return
                        result 
                        |> Seq.map unwrapper
                        |> Seq.map (fun evt -> 
                                        let metadata = { SourceMessageId = (Guid.NewGuid()); MessageId = (Guid.NewGuid()) }
                                        (stream, evt, metadata))
                        |> List.ofSeq
                }

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
            | _ -> return NonEmptyList.singleton (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>) |> Choice2Of2
        }
        
    let ToInterface<'TId,'TCmd,'TState,'TEvent when 'TId :> IIdentity> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) = {
        new ICommandHandler<'TEvent,'TId> with 
             member this.GetId cmd = untypedGetId sb cmd
             member this.CmdType = typeof<'TCmd>
             member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add sb.StateBuilder
             member this.Handler aggregateConfig cmd = handleCommand sb aggregateConfig cmd
        }

    let buildCmd<'TId,'TCmd,'TState,'TEvent when 'TId :> IIdentity> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) : IHandler<'TEvent,'TId> = {
            new IHandler<'TEvent,'TId> with
                member x.add handlers =
                    let cmdInterface = ToInterface sb
                    handlers.AddCommandHandler cmdInterface
        }

    let addValidator 
        (validator : Validator<'TCmd,'TState>) 
        (handler: CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) = 
        { handler with Validators = validator::handler.Validators }

    let ensureFirstCommand x = addValidator (StateValidator (isNone id "Must be the first command")) x

    let buildSimpleCmdHandler<'TId,'TCmd,'TCmdState,'TEvent when 'TId :> IIdentity> stateBuilder = 
        (simpleHandler<'TId,'TCmdState,'TCmd,'TEvent> stateBuilder) >> buildCmd
        
    let getEventInterfaceForLink<'TLinkEvent,'TEvent,'TId when 'TId :> IIdentity> (fId : 'TLinkEvent -> 'TId) = {
        new IEventHandler<'TEvent,'TId> with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder
             member this.Handler aggregateConfig sourceStream sourceEventNumber (evt : EventStreamEventData) = eventStream {
                let metadata = { SourceMessageId = System.Guid.NewGuid(); MessageId = System.Guid.NewGuid() }

                let resultingStream = aggregateConfig.GetEventStreamName (fId (evt.Body :?> 'TLinkEvent))

                // todo: should not be new stream
                let! _ = EventStream.writeLink resultingStream NewStream sourceStream sourceEventNumber metadata
                return ()
             }
        }

    let getEventInterfaceForOnEvent<'TOnEvent, 'TEvent, 'TId, 'TState when 'TId :> IIdentity> (fId : 'TOnEvent -> 'TId) (stateBuilder : NamedStateBuilder<'TState>) (runEvent : 'TOnEvent -> seq<'TEvent>) = {
        new IEventHandler<'TEvent,'TId> with 
            member this.EventType = typeof<'TOnEvent>
            member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add stateBuilder
            member this.Handler aggregateConfig sourceStream sourceEventNumber evt = eventStream {
                let unwrapper = MagicMapper.getUnwrapper<'TEvent>()
                let typedEvent = evt.Body :?> 'TOnEvent
                let resultingStream = aggregateConfig.GetEventStreamName (fId typedEvent)

                let! (eventsConsumed, state) = getChildState aggregateConfig.StateBuilder stateBuilder resultingStream

                let! eventTypeMap = getEventTypeMap()

                let resultingEvents = 
                    runEvent typedEvent
                    |> Seq.map (fun x -> 
                        let metadata = { SourceMessageId = (Guid.NewGuid()); MessageId = (Guid.NewGuid()) }
                        let event = unwrapper x
                        let eventType = eventTypeMap.FindValue (new ComparableType(event.GetType()))
                        Event { Body = event; EventType = eventType; Metadata = metadata })

                let! _ = EventStream.writeToStream resultingStream NewStream resultingEvents
                return ()
            }
    }

    let linkEvent<'TLinkEvent,'TEvent,'TId when 'TId :> IIdentity> fId (linkEvent : 'TLinkEvent -> 'TEvent) = {
        new IHandler<'TEvent,'TId> with
            member x.add handlers =
                let linkerInterface = (getEventInterfaceForLink<'TLinkEvent,'TEvent,'TId> fId)
                handlers.AddEventHandler linkerInterface
    }

    let onEvent<'TOnEvent,'TEvent,'TEventState,'TId when 'TId :> IIdentity> fId (stateBuilder : NamedStateBuilder<'TEventState>) (runEvent : 'TOnEvent -> seq<'TEvent>) = {
        new IHandler<'TEvent,'TId> with
            member x.add handlers =
                let onEventInterface = (getEventInterfaceForOnEvent<'TOnEvent,'TEvent,'TId,'TEventState> fId stateBuilder runEvent)
                handlers.AddEventHandler onEventInterface
    }

type AggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext when 'TId :> IIdentity> = {
    Configuration : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId>
    Handlers : AggregateHandlers<'TEvents, 'TId>
}

module Aggregate = 
    type AggregateBuilder<'TEvent,'TId when 'TId :> IIdentity> () = 
        member this.Zero() = AggregateHandlers<'TEvent,'TId>.Empty

        member x.Delay(f : unit -> AggregateHandlers<'TEvent,'TId>) = f ()

        member this.Yield(x:IHandler<'TEvent,'TId>) :  AggregateHandlers<'TEvent,'TId> =
            let empty = AggregateHandlers<'TEvent,'TId>.Empty 
            let result = x.add empty
            result

        member this.Combine (a:AggregateHandlers<'TEvent,'TId>,b:AggregateHandlers<'TEvent,'TId>) =
            a.Combine b

    let aggregate<'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity> aggregateType =
        new AggregateBuilder<'TEvent,'TId>()

    let toAggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext when 'TId :> IIdentity>
        (getCommandStreamName : 'TId -> string)
        (getEventStreamName : 'TId -> string) 
        (handlers : AggregateHandlers<'TEvents,'TId>) = 

            let commandStateBuilders = 
                handlers.CommandHandlers |> Seq.map (fun x -> x.AddStateBuilder)
            let eventStateBuilders =
                handlers.EventHandlers |> Seq.map (fun x -> x.AddStateBuilder)

            let combinedAggregateStateBuilder = 
                commandStateBuilders
                |> Seq.append eventStateBuilders
                |> Seq.fold (|>) CombinedStateBuilder.empty

            let config = {
                StateBuilder = combinedAggregateStateBuilder
                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
            }

            {
                Configuration = config
                Handlers = handlers
            }