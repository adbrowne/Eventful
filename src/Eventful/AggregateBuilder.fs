namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

open Eventful.EventStream

type CommandResult = Choice<list<string * obj * EventMetadata>,NonEmptyList<ValidationFailure>> 
type StreamNameBuilder<'TId> = ('TId -> string)

type EventResult = unit

type IAggregateType =
    abstract Name : string with get

type AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateType, 'TAggregateId> = {
    // the combination of all the named state builders
    // for commands and events
    StateBuilder : CombinedStateBuilder
    // name of the aggregate type - can be used to build
    // stream name
    AggregateType : string

    GetCommandStreamName : 'TCommandContext -> 'TAggregateType -> 'TAggregateId -> string
    GetEventStreamName : 'TEventContext -> 'TAggregateType -> 'TAggregateId -> string
}

type ICommandHandler<'TEvent,'TId when 'TId :> IIdentity> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : CombinedStateBuilder -> CombinedStateBuilder
    abstract member GetId : obj -> 'TId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateType, 'TAggregateId> -> string -> obj -> EventStreamProgram<CommandResult>

type IEventHandler<'TEvent,'TId> =
    abstract member AddStateBuilder : CombinedStateBuilder -> CombinedStateBuilder
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateType, 'TAggregateId> -> string -> string -> int -> EventStreamEventData -> EventStreamProgram<EventResult>

type AggregateHandlers<'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity and 'TAggregateType :> IAggregateType> private 
    (
        aggregateType : 'TAggregateType,
        commandHandlers : list<ICommandHandler<'TEvent,'TId>>, 
        eventHandlers : list<IEventHandler<'TEvent,'TId>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AggregateType = aggregateType
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TEvent,'TId, 'TAggregateType>(aggregateType, handler::commandHandlers, eventHandlers)
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TEvent,'TId, 'TAggregateType>(aggregateType, commandHandlers, handler::eventHandlers)
    member x.Combine (y:AggregateHandlers<_,_,_>) =
        new AggregateHandlers<_,_,_>(
            aggregateType,
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers)

    static member Empty aggregateType = new AggregateHandlers<'TEvent,'TId, 'TAggregateType>(aggregateType, List.empty, List.empty)
    
type IHandler<'TEvent,'TId when 'TId :> IIdentity> = 
    abstract member add : AggregateHandlers<'TEvent,'TId, 'TAggregateType> -> AggregateHandlers<'TEvent,'TId, 'TAggregateType>

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

    let getStreamName aggregateName (id : IIdentity) =
        sprintf "%s-%s" aggregateName (id.GetId)

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

    let handleCommand (commandHandler:CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) (aggregateConfiguration : AggregateConfiguration<_,_,_,_>) aggregateType (cmd : obj) =
        let unwrapper = MagicMapper.getUnwrapper<'TEvent>()
        eventStream {
            match cmd with
            | :? 'TCmd as cmd -> 
                let id = commandHandler.GetId cmd
                let stream = getStreamName aggregateType id
                
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
             member this.Handler aggregateConfig aggregateType cmd = handleCommand sb aggregateConfig aggregateType cmd
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
             member this.Handler aggregateConfig aggregateType sourceStream sourceEventNumber (evt : EventStreamEventData) = eventStream {
                let metadata = { SourceMessageId = System.Guid.NewGuid(); MessageId = System.Guid.NewGuid() }

                let resultingStream = getStreamName aggregateType (fId (evt.Body :?> 'TLinkEvent))

                // todo: should not be new stream
                let! _ = EventStream.writeLink resultingStream NewStream sourceStream sourceEventNumber metadata
                return ()
             }
        }

    let getEventInterfaceForOnEvent<'TOnEvent, 'TEvent, 'TId, 'TState when 'TId :> IIdentity> (fId : 'TOnEvent -> 'TId) (stateBuilder : NamedStateBuilder<'TState>) (runEvent : 'TOnEvent -> seq<'TEvent>) = {
        new IEventHandler<'TEvent,'TId> with 
            member this.EventType = typeof<'TOnEvent>
            member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add stateBuilder
            member this.Handler aggregateConfig aggregateType sourceStream sourceEventNumber evt = eventStream {
                let unwrapper = MagicMapper.getUnwrapper<'TEvent>()
                let typedEvent = evt.Body :?> 'TOnEvent
                let resultingStream = getStreamName aggregateType (fId typedEvent)

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

type AggregateDefinition<'TEvents, 'TId, 'TAggregateType, 'TCommandContext, 'TEventContext, 'TAggregateId when 'TId :> IIdentity and 'TAggregateType :> IAggregateType> = {
    Configuration : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateType, 'TAggregateId>
    Handlers : AggregateHandlers<'TEvents, 'TId, 'TAggregateType>
}

module Aggregate = 
    type AggregateBuilder<'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity and 'TAggregateType :> IAggregateType> (aggregateType : 'TAggregateType) = 
        member this.Zero() = AggregateHandlers<'TEvent,'TId, 'TAggregateType>.Empty

        member x.Delay(f : unit -> AggregateHandlers<'TEvent,'TId, 'TAggregateType>) = f ()

        member this.Yield(x:IHandler<'TEvent,'TId>) :  AggregateHandlers<'TEvent,'TId, 'TAggregateType> =
            let empty = AggregateHandlers<'TEvent,'TId, 'TAggregateType>.Empty aggregateType
            let result = x.add empty
            result

        member this.Combine (a:AggregateHandlers<'TEvent,'TId, 'TAggregateType>,b:AggregateHandlers<'TEvent,'TId, 'TAggregateType>) =
            a.Combine b

    let aggregate<'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity and 'TAggregateType :> IAggregateType> aggregateType =
        new AggregateBuilder<'TEvent,'TId, 'TAggregateType>(aggregateType)

    let toAggregateDefinition<'TEvents, 'TId, 'TAggregateType, 'TCommandContext, 'TEventContext, 'TAggregateId when 'TId :> IIdentity and 'TAggregateType :> IAggregateType>
        (getCommandStreamName : 'TCommandContext -> 'TAggregateType -> 'TAggregateId -> string)
        (getEventStreamName : 'TEventContext -> 'TAggregateType -> 'TAggregateId -> string) 
        (handlers : AggregateHandlers<'TEvents,'TId, 'TAggregateType>) = 

            let commandStateBuilders = 
                handlers.CommandHandlers |> Seq.map (fun x -> x.AddStateBuilder)
            let eventStateBuilders =
                handlers.EventHandlers |> Seq.map (fun x -> x.AddStateBuilder)

            let combinedAggregateStateBuilder = 
                commandStateBuilders
                |> Seq.append eventStateBuilders
                |> Seq.fold (|>) CombinedStateBuilder.empty

            let aggregateTypeString = handlers.AggregateType.Name

            let config = {
                StateBuilder = combinedAggregateStateBuilder
                // name of the aggregate type - can be used to build
                // stream name
                AggregateType = aggregateTypeString

                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
            }

            {
                Configuration = config
                Handlers = handlers
            }