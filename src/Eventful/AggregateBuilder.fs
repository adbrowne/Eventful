namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections
open FSharp.Control

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
| AlreadyProcessed // idempotency check failed
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
}

type SystemConfiguration<'TId, 'TMetadata> = {
    //used to make processing idempotent
    GetUniqueId : 'TMetadata -> string option 
    GetAggregateId : 'TMetadata -> 'TId
}

type ICommandHandler<'TId,'TCommandContext, 'TMetadata> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, 'TId> list -> IStateBlockBuilder<'TMetadata, 'TId> list
    abstract member GetId : 'TCommandContext-> obj -> 'TId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata> -> 'TCommandContext -> obj -> EventStreamProgram<CommandResult<'TMetadata>,'TMetadata>
    abstract member Visitable : IRegistrationVisitable

type IEventHandler<'TId,'TMetadata, 'TEventContext when 'TId : equality> =
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, 'TId> list -> IStateBlockBuilder<'TMetadata, 'TId> list
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata> -> 'TEventContext -> string -> int -> EventStreamEventData<'TMetadata> -> Async<EventStreamProgram<EventResult,'TMetadata>>

type AggregateCommandHandlers<'TId,'TCommandContext,'TMetadata> = seq<ICommandHandler<'TId,'TCommandContext,'TMetadata>>
type AggregateEventHandlers<'TId,'TMetadata, 'TEventContext  when 'TId : equality> = seq<IEventHandler<'TId,'TMetadata, 'TEventContext >>

type AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata when 'TId : equality> private 
    (
        commandHandlers : list<ICommandHandler<'TId,'TCommandContext,'TMetadata>>, 
        eventHandlers : list<IEventHandler<'TId,'TMetadata, 'TEventContext>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata>(handler::commandHandlers, eventHandlers)
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata>(commandHandlers, handler::eventHandlers)
    member x.Combine (y:AggregateHandlers<_,_,_,_>) =
        new AggregateHandlers<_,_,_,_>(
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers)

    static member Empty = new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata>(List.empty, List.empty)
    
type IHandler<'TEvent,'TId,'TCommandContext,'TEventContext when 'TId : equality> = 
    abstract member add : AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata> -> AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata>

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

type metadataBuilder<'TAggregateId,'TMetadata> = 'TAggregateId -> Guid -> string -> 'TMetadata

type CommandHandlerOutput<'TAggregateId,'TMetadata> = {
    UniqueId : string // used to make commands idempotent
    Events : seq<obj * metadataBuilder<'TAggregateId,'TMetadata>>
}

type CommandHandler<'TCmd, 'TCommandContext, 'TCommandState, 'TAggregateId, 'TMetadata, 'TValidatedState> = {
    GetId : 'TCommandContext -> 'TCmd -> 'TAggregateId
    StateBuilder : IStateBuilder<'TCommandState, 'TMetadata, 'TAggregateId>
    StateValidation : 'TCommandState -> Choice<'TValidatedState, NonEmptyList<ValidationFailure>> 
    Validators : Validator<'TCmd,'TCommandState,'TMetadata,'TAggregateId> list
    Handler : 'TValidatedState -> 'TCommandContext -> 'TCmd -> Async<Choice<CommandHandlerOutput<'TAggregateId,'TMetadata>, NonEmptyList<ValidationFailure>>>
    SystemConfiguration : SystemConfiguration<'TAggregateId, 'TMetadata>
}

type MultiEventRun<'TAggregateId,'TMetadata,'TState when 'TAggregateId : equality> = ('TAggregateId * ('TState -> seq<obj * metadataBuilder<'TAggregateId,'TMetadata>>))

open Eventful.Validation

module AggregateActionBuilder =
    open EventStream

    let log = createLogger "Eventful.AggregateActionBuilder"

    let fullHandlerAsync<'TId, 'TState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> systemConfiguration stateBuilder f =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = f
            SystemConfiguration = systemConfiguration
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata,'TState> 

    let fullHandler<'TId, 'TState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> systemConfiguration stateBuilder f =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = (fun a b c ->  f a b c |> Async.returnM)
            SystemConfiguration = systemConfiguration
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata,'TState> 

    let simpleHandler<'TAggregateId, 'TState, 'TCmd, 'TCommandContext, 'TEvent, 'TMetadata> systemConfiguration stateBuilder (f : 'TCmd -> (string * 'TEvent * metadataBuilder<'TAggregateId,'TMetadata>)) =
        let makeResult (uniqueId, evt, metadata) = { 
            UniqueId = uniqueId 
            Events =  (evt :> obj, metadata) |> Seq.singleton 
        }

        {
            GetId = (fun _ -> MagicMapper.magicId<'TAggregateId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = (fun _ _ -> f >> makeResult >> Success >> Async.returnM)
            SystemConfiguration = systemConfiguration
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TAggregateId, 'TMetadata,'TState> 

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

    let uniqueIdBuilder (systemConfiguration: SystemConfiguration<'TId,'TMetadata>) =
        StateBuilder.Empty "$Eventful::UniqueIdBuilder" Set.empty
        |> StateBuilder.allEventsHandler 
            (fun m -> systemConfiguration.GetAggregateId m) 
            (fun (s,e,m) -> 
                match systemConfiguration.GetUniqueId m with
                | Some uniqueId ->
                    s |> Set.add uniqueId
                | None -> s)
        |> StateBuilder.toInterface

    let inline runCommand 
        (systemConfiguration: SystemConfiguration<'TAggregateId,'TMetadata>) 
        stream 
        (eventsConsumed, combinedState) 
        (commandStateBuilder : IStateBuilder<'TChildState, 'TMetadata, 'TAggregateId>) 
        (aggregateId : 'TAggregateId) 
        f = 
        eventStream {
            let commandState = commandStateBuilder.GetState combinedState

            let! result = runAsync <| f commandState

            return! 
                match result with
                | Choice1Of2 (r : CommandHandlerOutput<'TAggregateId,'TMetadata>) -> 
                    eventStream {
                        let uniqueIds = (uniqueIdBuilder systemConfiguration).GetState combinedState
                        if uniqueIds |> Set.contains r.UniqueId then
                            return Choice2Of2 CommandRunFailure<_>.AlreadyProcessed
                        else
                            let events = 
                                r.Events
                                |> Seq.map (fun (evt, metadata) -> 
                                                let metadata = 
                                                    metadata aggregateId (Guid.NewGuid()) r.UniqueId
                                                (stream, evt, metadata))
                                |> List.ofSeq

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
        | AlreadyProcessed ->
            CommandFailure.CommandError "AlreadyProcessed"
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
        let processCommand cmd (unitStates : Map<string,obj>) commandState = async {
            let validatedChoice = runValidation commandHandler.Validators cmd unitStates
            match validatedChoice with
            | Choice1Of2 validated ->
                let validatedStateChoice = commandHandler.StateValidation commandState
                match validatedStateChoice with
                | Choice1Of2 validatedState ->
                    return! commandHandler.Handler validatedState commandContext validated
                | Choice2Of2 errors ->
                    return Choice2Of2 errors
            | Choice2Of2 errors ->
                return Choice2Of2 errors
        }

        let systemConfiguration = commandHandler.SystemConfiguration

        match cmd with
        | :? 'TCmd as cmd -> 
            let getId = FSharpx.Choice.protect (commandHandler.GetId commandContext) cmd
            match getId with
            | Choice1Of2 aggregateId ->
                let stream = aggregateConfiguration.GetCommandStreamName commandContext aggregateId

                let getResult = eventStream {
                    let! (eventsConsumed, combinedState) = 
                        aggregateConfiguration.StateBuilder
                        |> AggregateStateBuilder.combineHandlers (getValidatorsUnitBuilders commandHandler.Validators)
                        |> AggregateStateBuilder.combineHandlers ((uniqueIdBuilder systemConfiguration).GetBlockBuilders)
                        |> AggregateStateBuilder.ofStateBuilderList
                        |> AggregateStateBuilder.toStreamProgram stream aggregateId
                    let! result = 
                        runCommand 
                            systemConfiguration 
                            stream 
                            (eventsConsumed, combinedState) 
                            commandHandler.StateBuilder 
                            aggregateId 
                            (processCommand cmd combinedState)
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

    let buildSimpleCmdHandler<'TId,'TCmd,'TCmdState,'TCommandContext,'TEvent, 'TMetadata when 'TId : equality> systemConfiguration stateBuilder = 
        (simpleHandler<'TId,'TCmdState,'TCmd,'TCommandContext,'TEvent, 'TMetadata> systemConfiguration stateBuilder) >> buildCmd
        
    let getEventInterfaceForLink<'TLinkEvent,'TAggregateId,'TMetadata, 'TCommandContext, 'TEventContext when 'TAggregateId : equality> (fId : 'TLinkEvent -> 'TAggregateId) (metadataBuilder : metadataBuilder<'TAggregateId,'TMetadata>) = {
        new IEventHandler<'TAggregateId,'TMetadata, 'TEventContext > with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder builders = builders
             member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber (evt : EventStreamEventData<'TMetadata>) = 
                eventStream {
                    let aggregateId = fId (evt.Body :?> 'TLinkEvent)
                    let metadata =  
                        metadataBuilder aggregateId (Guid.NewGuid()) (Guid.NewGuid().ToString()) 

                    let resultingStream = aggregateConfig.GetEventStreamName eventContext aggregateId

                    let! result = EventStream.writeLink resultingStream Any sourceStream sourceEventNumber metadata

                    return 
                        match result with
                        | WriteResult.WriteSuccess _ -> 
                            log.Debug <| lazy (sprintf "Wrote Link To %A %A %A" DateTime.Now.Ticks sourceStream sourceEventNumber)
                            ()
                        | WriteResult.WrongExpectedVersion -> failwith "WrongExpectedVersion writing event. TODO: retry"
                        | WriteResult.WriteError ex -> failwith <| sprintf "WriteError writing event: %A" ex
                        | WriteResult.WriteCancelled -> failwith "WriteCancelled writing event" } 
                |> Async.returnM
    } 

    let writeEvents aggregateConfig eventsConsumed eventContext aggregateId evts  = eventStream {
        let resultingStream = aggregateConfig.GetEventStreamName eventContext aggregateId

        let! resultingEvents = 
            evts
            |> Seq.toList
            |> EventStream.mapM (fun (event,metadata) -> 
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

    let processSequence
        (aggregateConfig : AggregateConfiguration<'TCommandContext,'TEventContext,'TId,'TMetadata>)
        (stateBuilder : IStateBuilder<'TState,_,_>)
        (eventContext : 'TEventContext)  
        (handlers : AsyncSeq<MultiEventRun<'TId,'TMetadata,'TState>>) 
        =
        handlers
        |> AsyncSeq.map (fun (id, handler) ->
            eventStream {
                let resultingStream = aggregateConfig.GetEventStreamName eventContext id
                let! (eventsConsumed, combinedState) = 
                    aggregateConfig.StateBuilder 
                    |> AggregateStateBuilder.ofStateBuilderList
                    |> AggregateStateBuilder.toStreamProgram resultingStream id
                let state = stateBuilder.GetState combinedState
                let evts = handler state
                return! writeEvents aggregateConfig eventsConsumed eventContext id evts
            }
        )

    let getEventInterfaceForOnEvent<'TOnEvent, 'TEvent, 'TId, 'TState, 'TMetadata, 'TCommandContext, 'TEventContext when 'TId : equality> (stateBuilder: IStateBuilder<_,_,_>) (fId : ('TOnEvent * 'TEventContext) -> AsyncSeq<MultiEventRun<'TId,'TMetadata,'TState>>) = {
        new IEventHandler<'TId,'TMetadata, 'TEventContext> with 
            member this.EventType = typeof<'TOnEvent>
            member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers stateBuilder.GetBlockBuilders builders
            member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber evt = 
                let typedEvent = evt.Body :?> 'TOnEvent

                let asyncSeq = fId (typedEvent, eventContext)

                let handlerSequence = processSequence aggregateConfig stateBuilder eventContext asyncSeq

                let acc existingP p =
                    eventStream {
                        do! existingP
                        do! p 
                    }

                handlerSequence |> AsyncSeq.fold acc EventStream.empty
    }

    let linkEvent<'TLinkEvent,'TId,'TCommandContext,'TEventContext,'TMetadata when 'TId : equality> fId (metadata : metadataBuilder<'TId,'TMetadata>) = 
        getEventInterfaceForLink<'TLinkEvent,'TId,'TMetadata,'TCommandContext,'TEventContext> fId metadata

    let onEventMultiAsync<'TOnEvent,'TEvent,'TEventState,'TId, 'TMetadata, 'TCommandContext,'TEventContext when 'TId : equality> 
        stateBuilder 
        (fId : ('TOnEvent * 'TEventContext) -> AsyncSeq<MultiEventRun<'TId,'TMetadata,'TEventState>>) = 
        getEventInterfaceForOnEvent<'TOnEvent,'TEvent,'TId,'TEventState, 'TMetadata, 'TCommandContext,'TEventContext> stateBuilder fId

    let onEventMulti<'TOnEvent,'TEvent,'TEventState,'TId, 'TMetadata, 'TCommandContext,'TEventContext when 'TId : equality> 
        stateBuilder 
        (fId : ('TOnEvent * 'TEventContext) -> seq<MultiEventRun<'TId,'TMetadata,'TEventState>>) = 
        onEventMultiAsync stateBuilder (fId >> AsyncSeq.ofSeq)

    let onEvent<'TOnEvent,'TEventState,'TId,'TMetadata,'TEventContext when 'TId : equality> 
        (fId : 'TOnEvent -> 'TEventContext -> 'TId) 
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TId>) 
        (runEvent : 'TEventState -> 'TOnEvent -> seq<obj * metadataBuilder<'TId,'TMetadata>>) = 
        
        let runEvent' = fun evt state ->
            runEvent state evt

        let handler = 
            (fun (evt, eventCtx) -> 
                let aggId = fId evt eventCtx
                let h = runEvent' evt
                (aggId, h) |> Seq.singleton
            )
        onEventMulti stateBuilder handler

type AggregateDefinition<'TId, 'TCommandContext, 'TEventContext, 'TMetadata when 'TId : equality> = {
    Configuration : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata>
    Handlers : AggregateHandlers<'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
}

module Aggregate = 
    let toAggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata when 'TId : equality>
        (getCommandStreamName : 'TCommandContext -> 'TId -> string)
        (getEventStreamName : 'TEventContext -> 'TId -> string) 
        (commandHandlers : AggregateCommandHandlers<'TId,'TCommandContext, 'TMetadata>)
        (eventHandlers : AggregateEventHandlers<'TId,'TMetadata, 'TEventContext >) = 

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
            }

            let handlers =
                commandHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_>) h -> x.AddCommandHandler h) AggregateHandlers.Empty

            let handlers =
                eventHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_>) h -> x.AddEventHandler h) handlers

            {
                Configuration = config
                Handlers = handlers
            }