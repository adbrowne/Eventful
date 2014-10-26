namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

open Eventful.EventStream

type CommandSuccess<'TMetadata> = {
    Events : (string * obj * 'TMetadata) list
    Position : EventPosition option
}

type CommandResult<'TMetadata> = Choice<CommandSuccess<'TMetadata>,NonEmptyList<CommandFailure>> 

type CommandRunFailure<'a> =
| HandlerError of 'a
| WrongExpectedVersion
| WriteCancelled
| WriteError of System.Exception
| Exception of exn

type StreamNameBuilder<'TId> = ('TId -> string)

type IRegistrationVisitor<'T,'U> =
    abstract member Visit<'TCmd> : 'T -> 'U

type IRegistrationVisitable =
    abstract member Receive<'T,'U> : 'T -> IRegistrationVisitor<'T,'U> -> 'U

type EventResult = unit

type AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateId, 'TMetadata> = {
    // the combination of all the named state builders
    // for commands and events
    StateBuilder : IStateBlockBuilder<'TMetadata, 'TAggregateId> list

    GetCommandStreamName : 'TCommandContext -> 'TAggregateId -> string
    GetEventStreamName : 'TEventContext -> 'TAggregateId -> string
    GetAggregateId : 'TAggregateId -> Guid
}

type ICommandHandler<'TId,'TCommandContext, 'TMetadata> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, 'TId> list -> IStateBlockBuilder<'TMetadata, 'TId> list
    abstract member GetId : 'TCommandContext-> obj -> 'TId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata> -> 'TCommandContext -> obj -> EventStreamProgram<CommandResult<'TMetadata>,'TMetadata>
    abstract member Visitable : IRegistrationVisitable

type IEventHandler<'TId,'TMetadata> =
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, 'TId> list -> IStateBlockBuilder<'TMetadata, 'TId> list
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata> -> 'TEventContext -> string -> int -> EventStreamEventData<'TMetadata> -> EventStreamProgram<EventResult,'TMetadata>

type AggregateCommandHandlers<'TId,'TCommandContext,'TMetadata> = seq<ICommandHandler<'TId,'TCommandContext,'TMetadata>>
type AggregateEventHandlers<'TId,'TMetadata> = seq<IEventHandler<'TId,'TMetadata>>

type AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> private 
    (
        commandHandlers : list<ICommandHandler<'TId,'TCommandContext,'TMetadata>>, 
        eventHandlers : list<IEventHandler<'TId,'TMetadata>>
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

type IStateValidatorRegistration<'TMetadata,'TKey> =
    abstract member Register<'TState> : IStateBuilder<'TState, 'TMetadata, 'TKey>  -> ('TState -> seq<ValidationFailure>) -> (IStateBlockBuilder<'TMetadata, 'TKey> list * (Map<string,obj> -> seq<ValidationFailure>))

type ICombinedValidatorRegistration<'TMetadata,'TKey> =
    abstract member Register<'TState,'TInput> : IStateBuilder<'TState, 'TMetadata, 'TKey>  -> ('TInput -> 'TState -> seq<ValidationFailure>) -> (IStateBlockBuilder<'TMetadata, 'TKey> list * ('TInput -> Map<string,obj> -> seq<ValidationFailure>))

type Validator<'TCmd,'TState, 'TMetadata, 'TKey> = 
| CommandValidator of ('TCmd -> seq<ValidationFailure>)
| StateValidator of (IStateValidatorRegistration<'TMetadata,'TKey> -> (IStateBlockBuilder<'TMetadata, 'TKey> list * (Map<string,obj> -> seq<ValidationFailure>)))
| CombinedValidator of (ICombinedValidatorRegistration<'TMetadata,'TKey> -> (IStateBlockBuilder<'TMetadata, 'TKey> list * ('TCmd -> Map<string,obj> -> seq<ValidationFailure>)))

type metadataBuilder<'TMetadata> = Guid -> Guid -> string -> 'TMetadata

type CommandHandler<'TCmd, 'TCommandContext, 'TCommandState, 'TId, 'TMetadata, 'TValidatedState> = {
    GetId : 'TCommandContext -> 'TCmd -> 'TId
    StateBuilder : IStateBuilder<'TCommandState, 'TMetadata, 'TId>
    StateValidation : 'TCommandState -> Choice<'TValidatedState, NonEmptyList<ValidationFailure>> 
    Validators : Validator<'TCmd,'TCommandState,'TMetadata,'TId> list
    Handler : 'TValidatedState -> 'TCommandContext -> 'TCmd -> Choice<seq<obj * metadataBuilder<'TMetadata>>, NonEmptyList<ValidationFailure>>
}

open Eventful.EventStream
open Eventful.Validation

module AggregateActionBuilder =
    open EventStream

    let log = createLogger "Eventful.AggregateActionBuilder"

    let fullHandler<'TId, 'TState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> stateBuilder f =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = f
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata,'TState> 

    let simpleHandler<'TId, 'TState, 'TCmd, 'TCommandContext, 'TEvent, 'TMetadata> stateBuilder (f : 'TCmd -> ('TEvent * metadataBuilder<'TMetadata>)) =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = (fun _ _ -> f >> (fun (evt,metadata) -> (evt :> obj, metadata)) >> Seq.singleton >> Success)
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata,'TState> 

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
                        | StateValidator validatorRegistration -> 
                            let registration = 
                                { new IStateValidatorRegistration<_,_> with 
                                    member x.Register stateBuilder validator = 
                                        let runValidation unitValueMap : seq<ValidationFailure> = 
                                            let state = stateBuilder.GetState unitValueMap
                                            validator state

                                        (stateBuilder.GetBlockBuilders, runValidation)
                                }

                            let (unitBuilders, runValidation) = validatorRegistration registration
                            
                            runValidation state |> (toChoiceValidator cmd)
                        | CombinedValidator validatorRegistration -> 
                            let registration = 
                                { new ICombinedValidatorRegistration<_,_> with 
                                    member x.Register stateBuilder validator = 
                                        let runValidation cmd unitValueMap : seq<ValidationFailure> = 
                                            let state = stateBuilder.GetState unitValueMap
                                            validator cmd state

                                        (stateBuilder.GetBlockBuilders, runValidation)
                                }

                            let (unitBuilders, runValidation) = validatorRegistration registration
                            
                            runValidation cmd state |> (toChoiceValidator cmd)
                     )
         |> List.map (fun x -> x)
         |> List.fold (fun s validator -> v.apl validator s) (Choice.returnM cmd) 

    let untypedGetId<'TId,'TCmd,'TEvent,'TState, 'TCommandContext, 'TMetadata, 'TValidatedState> (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TValidatedState>) (context : 'TCommandContext) (cmd:obj) =
        match cmd with
        | :? 'TCmd as cmd ->
            sb.GetId context cmd
        | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())

    let inline runCommand stream (eventsConsumed, combinedState) (commandStateBuilder : IStateBuilder<'TChildState, 'TMetadata, 'TId>) (id : Guid) f = 
        eventStream {
            let commandState = commandStateBuilder.GetState combinedState

            let result = 
                f commandState
                |> Choice.map (fun r ->
                    r
                    |> Seq.map (fun (evt, metadata) -> 
                                    let metadata = 
                                        metadata id (Guid.NewGuid()) (Guid.NewGuid().ToString())
                                    (stream, evt, metadata))
                    |> List.ofSeq)

            return! 
                match result with
                | Choice1Of2 events -> 
                    eventStream {
                        let rawEventToEventStreamEvent (_, event, metadata) = getEventStreamEvent event metadata
                        let! eventStreamEvents = EventStream.mapM rawEventToEventStreamEvent events

                        let expectedVersion = 
                            match eventsConsumed with
                            | 0 -> NewStream
                            | x -> AggregateVersion (x - 1)

                        let! writeResult = writeToStream stream expectedVersion eventStreamEvents
                        log.Debug <| lazy (sprintf "WriteResult: %A" writeResult)
                        
                        return 
                            match writeResult with
                            | WriteResult.WriteSuccess pos ->
                                Choice1Of2 {
                                    Events = events
                                    Position = Some pos
                                }
                            | WriteResult.WrongExpectedVersion -> 
                                Choice2Of2 CommandRunFailure<_>.WrongExpectedVersion
                            | WriteResult.WriteError ex -> 
                                Choice2Of2 <| CommandRunFailure<_>.WriteError ex
                            | WriteResult.WriteCancelled -> 
                                Choice2Of2 CommandRunFailure<_>.WriteCancelled

                    }
                | Choice2Of2 x ->
                    eventStream { 
                        return
                            HandlerError x
                            |> Choice2Of2
                    }
        }

    let getValidatorUnitBuilders (validator : Validator<'TCmd,'TCommandState,'TMetadata,'TId>)= 
        match validator with
        | CommandValidator validator -> []
        | StateValidator validatorRegistration -> 
            let registration = 
                { new IStateValidatorRegistration<_,_> with 
                    member x.Register stateBuilder validator = 
                        let runValidation unitValueMap : seq<ValidationFailure> = 
                            let state = stateBuilder.GetState unitValueMap
                            validator state

                        (stateBuilder.GetBlockBuilders, runValidation)
                }

            let (unitBuilders, _) = validatorRegistration registration
            
            unitBuilders
        | CombinedValidator validatorRegistration -> 
            let registration = 
                { new ICombinedValidatorRegistration<_,_> with 
                    member x.Register stateBuilder validator = 
                        let runValidation cmd unitValueMap : seq<ValidationFailure> = 
                            let state = stateBuilder.GetState unitValueMap
                            validator cmd state

                        (stateBuilder.GetBlockBuilders, runValidation)
                }

            let (unitBuilders, _) = validatorRegistration registration
            
            unitBuilders

    let getValidatorsUnitBuilders (validators : Validator<'TCmd,'TCommandState,'TMetadata,'TId> list) = 
        List.fold (fun s b -> List.append s (getValidatorUnitBuilders b)) [] validators

    let mapValidationFailureToCommandFailure (x : CommandRunFailure<_>) =
        match x with
        | HandlerError x -> 
            (NonEmptyList.map CommandFailure.ofValidationFailure) x
        | WrongExpectedVersion ->
            CommandFailure.CommandError "WrongExpectedVersion"
            |> NonEmptyList.singleton 
        | WriteCancelled ->
            CommandFailure.CommandError "WriteCancelled"
            |> NonEmptyList.singleton 
        | WriteError ex 
        | Exception ex ->
            CommandFailure.CommandException (None, ex)
            |> NonEmptyList.singleton 

    let retryOnWrongVersion f = eventStream {
        let maxTries = 100
        let retry = ref true
        let count = ref 0
        // WriteCancelled whould never be used
        let finalResult = ref (Choice2Of2 CommandRunFailure.WriteCancelled)
        while !retry do
            let! result = f
            match result with
            | Choice2Of2 WrongExpectedVersion ->
                count := !count + 1
                if !count < maxTries then
                    retry := true
                else
                    retry := false
                    finalResult := (Choice2Of2 WrongExpectedVersion)
            | x -> 
                retry := false
                finalResult := x

        return !finalResult
    }

    let handleCommand 
        (commandHandler:CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TValidatedState>) 
        (aggregateConfiguration : AggregateConfiguration<_,_,_,_>) 
        (commandContext : 'TCommandContext) 
        (cmd : obj) =
        let processCommand cmd (unitStates : Map<string,obj>) commandState =
            choose {
                let! validated = runValidation commandHandler.Validators cmd unitStates
                let! validatedState = commandHandler.StateValidation commandState
                return! commandHandler.Handler validatedState commandContext validated
            }

        match cmd with
        | :? 'TCmd as cmd -> 
            let getId = FSharpx.Choice.protect (commandHandler.GetId commandContext) cmd
            match getId with
            | Choice1Of2 id ->
                let stream = aggregateConfiguration.GetCommandStreamName commandContext id

                let getResult = eventStream {
                    let! (eventsConsumed, combinedState) = 
                        aggregateConfiguration.StateBuilder
                        |> AggregateStateBuilder.combineHandlers (getValidatorsUnitBuilders commandHandler.Validators)
                        |> AggregateStateBuilder.ofStateBuilderList
                        |> AggregateStateBuilder.toStreamProgram stream id
                    let! result = 
                        let aggregateGuid = aggregateConfiguration.GetAggregateId id
                        runCommand stream (eventsConsumed, combinedState) commandHandler.StateBuilder aggregateGuid (processCommand cmd combinedState)
                    return result
                }

                eventStream {
                    let! result = retryOnWrongVersion getResult
                    return
                        result |> Choice.mapSecond mapValidationFailureToCommandFailure
                }
            | Choice2Of2 exn ->
                eventStream {
                    return 
                        (Some "Retrieving aggregate id from command",exn)
                        |> CommandException
                        |> NonEmptyList.singleton 
                        |> Choice2Of2
                }
        | _ -> 
            eventStream {
                return 
                    (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>)
                    |> CommandError
                    |> NonEmptyList.singleton 
                    |> Choice2Of2
            }
        
    let ToInterface (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata,'TValidatedState>) = {
        new ICommandHandler<'TId,'TCommandContext,'TMetadata> with 
             member this.GetId context cmd = untypedGetId sb context cmd
             member this.CmdType = typeof<'TCmd>
             member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers sb.StateBuilder.GetBlockBuilders builders
             member this.Handler aggregateConfig commandContext cmd = handleCommand sb aggregateConfig commandContext cmd
             member this.Visitable = {
                new IRegistrationVisitable with
                    member x.Receive a r = r.Visit<'TCmd>(a)
             }
        }

    let withCmdId (getId : 'TCmd -> 'TId) (builder : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TValidatedState>) = 
        { builder with GetId = (fun _ cmd -> getId cmd )}

    let buildCmd (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TValidatedState>) : ICommandHandler<'TId,'TCommandContext,'TMetadata> = 
        ToInterface sb

    let addValidator 
        (validator : Validator<'TCmd,'TState, 'TMetadata, 'TId>) 
        (handler: CommandHandler<'TCmd,'TCommandContext, 'TState, 'TId, 'TMetadata, 'TValidatedState>) = 
        { handler with Validators = validator::handler.Validators }

    let buildSimpleCmdHandler<'TId,'TCmd,'TCmdState,'TCommandContext,'TEvent, 'TMetadata when 'TId : equality> stateBuilder = 
        (simpleHandler<'TId,'TCmdState,'TCmd,'TCommandContext,'TEvent, 'TMetadata> stateBuilder) >> buildCmd
        
    let getEventInterfaceForLink<'TLinkEvent,'TId,'TMetadata> (fId : 'TLinkEvent -> 'TId) (metadataBuilder : metadataBuilder<'TMetadata>) = {
        new IEventHandler<'TId,'TMetadata> with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder builders = builders
             member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber (evt : EventStreamEventData<'TMetadata>) = eventStream {
                let aggregateId = fId (evt.Body :?> 'TLinkEvent)
                let aggregateGuid = aggregateConfig.GetAggregateId aggregateId
                let metadata =  
                    metadataBuilder aggregateGuid (Guid.NewGuid()) (Guid.NewGuid().ToString()) 

                let resultingStream = aggregateConfig.GetEventStreamName eventContext aggregateId

                // todo: should not be new stream
                let! result = EventStream.writeLink resultingStream Any sourceStream sourceEventNumber metadata
                return 
                    match result with
                    | WriteResult.WriteSuccess _ -> 
                        log.Debug <| lazy (sprintf "Wrote Link To %A %A %A" DateTime.Now.Ticks sourceStream sourceEventNumber)
                        ()
                    | WriteResult.WrongExpectedVersion -> failwith "WrongExpectedVersion writing event. TODO: retry"
                    | WriteResult.WriteError ex -> failwith <| sprintf "WriteError writing event: %A" ex
                    | WriteResult.WriteCancelled -> failwith "WriteCancelled writing event"
             }
        }

    let getEventInterfaceForOnEvent<'TOnEvent, 'TEvent, 'TId, 'TState, 'TMetadata when 'TId : equality> (fId : 'TOnEvent -> 'TId seq) (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TId>) (runEvent : 'TId -> 'TState -> 'TOnEvent -> seq<'TEvent * metadataBuilder<'TMetadata>>) = {
        new IEventHandler<'TId,'TMetadata> with 
            member this.EventType = typeof<'TOnEvent>
            member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers stateBuilder.GetBlockBuilders builders
            member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber evt = eventStream {
                let typedEvent = evt.Body :?> 'TOnEvent
                let eventKeys = (fId typedEvent)
                for eventKey in eventKeys do
                    let resultingStream = aggregateConfig.GetEventStreamName eventContext eventKey

                    let! (eventsConsumed, combinedState) = 
                        aggregateConfig.StateBuilder 
                        |> AggregateStateBuilder.ofStateBuilderList
                        |> AggregateStateBuilder.toStreamProgram resultingStream eventKey
                    let state = stateBuilder.GetState combinedState

                    let! resultingEvents = 
                        runEvent eventKey state typedEvent
                        |> Seq.toList
                        |> EventStream.mapM (fun (event,metadata) -> 
                            let aggregateId = aggregateConfig.GetAggregateId eventKey
                            let metadata =  
                                metadata aggregateId (Guid.NewGuid()) (Guid.NewGuid().ToString())

                            getEventStreamEvent event metadata)

                    let expectedVersion = 
                        match eventsConsumed with
                        | 0 -> NewStream
                        | x -> AggregateVersion (x - 1)

                    let! result = EventStream.writeToStream resultingStream expectedVersion resultingEvents

                    return 
                        match result with
                        | WriteResult.WriteSuccess _ -> ()
                        | WriteResult.WrongExpectedVersion -> failwith "WrongExpectedVersion writing event. TODO: retry"
                        | WriteResult.WriteError ex -> failwith <| sprintf "WriteError writing event: %A" ex
                        | WriteResult.WriteCancelled -> failwith "WriteCancelled writing event"
            }
    }

    let linkEvent<'TLinkEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> fId (metadata : metadataBuilder<'TMetadata>) = 
        getEventInterfaceForLink<'TLinkEvent,'TId,'TMetadata> fId metadata

    let onEventMulti<'TOnEvent,'TEvent,'TEventState,'TId, 'TMetadata when 'TId : equality> fId (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TId>) (runEvent : 'TId -> 'TEventState -> 'TOnEvent -> seq<'TEvent * metadataBuilder<'TMetadata>>) = 
        getEventInterfaceForOnEvent<'TOnEvent,'TEvent,'TId,'TEventState, 'TMetadata> fId stateBuilder runEvent

    let onEvent<'TOnEvent,'TEvent,'TEventState,'TId, 'TMetadata when 'TId : equality> fId (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TId>) (runEvent : 'TEventState -> 'TOnEvent -> seq<'TEvent * metadataBuilder<'TMetadata>>) = 
        onEventMulti (fId >> Seq.singleton) stateBuilder (fun _ state evt -> runEvent state evt)


type AggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata> = {
    Configuration : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata>
    Handlers : AggregateHandlers<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
}

module Aggregate = 
    let toAggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
        (getCommandStreamName : 'TCommandContext -> 'TId -> string)
        (getEventStreamName : 'TEventContext -> 'TId -> string) 
        (getAggregateId : 'TId -> Guid) 
        (commandHandlers : AggregateCommandHandlers<'TId,'TCommandContext, 'TMetadata>)
        (eventHandlers : AggregateEventHandlers<'TId,'TMetadata>) = 

            let commandStateBuilders = 
                commandHandlers |> Seq.map (fun x -> x.AddStateBuilder)
            let eventStateBuilders =
                eventHandlers |> Seq.map (fun x -> x.AddStateBuilder)

            let combinedAggregateStateBuilder = 
                commandStateBuilders
                |> Seq.append eventStateBuilders
                |> Seq.fold (|>) []

            let config = {
                StateBuilder = combinedAggregateStateBuilder
                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
                GetAggregateId = getAggregateId
            }

            let handlers =
                commandHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddCommandHandler h) AggregateHandlers.Empty

            let handlers =
                eventHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddEventHandler h) handlers

            {
                Configuration = config
                Handlers = handlers
            }