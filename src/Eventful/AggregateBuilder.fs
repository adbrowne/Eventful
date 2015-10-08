namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Option
open FSharpx.Collections
open FSharp.Control

open Eventful.EventStream
open Eventful.MultiCommand

type CommandSuccess<'TBaseEvent, 'TMetadata> = {
    Events : (string * 'TBaseEvent * 'TMetadata) list
    Position : EventPosition option
}
with 
    static member Empty : CommandSuccess<'TBaseEvent, 'TMetadata> = {
        Events = List.empty
        Position = None
    }

type CommandResult<'TBaseEvent,'TMetadata> = Choice<CommandSuccess<'TBaseEvent,'TMetadata>,NonEmptyList<CommandFailure>> 

type StreamNameBuilder<'TId> = ('TId -> string)

type IRegistrationVisitor<'T,'U> =
    abstract member Visit<'TCmd> : 'T -> 'U

type IRegistrationVisitable =
    abstract member Receive<'T,'U> : 'T -> IRegistrationVisitor<'T,'U> -> 'U

type EventResult = unit

type metadataBuilder<'TMetadata> = string -> 'TMetadata

type IStateChangeHandler<'TMetadata, 'TBaseEvent> =
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, unit> list -> IStateBlockBuilder<'TMetadata, unit> list 
    abstract member Handler : StateSnapshot -> StateSnapshot -> seq<'TBaseEvent * 'TMetadata>

// 'TContext can either be CommandContext or EventContext
type AggregateConfiguration<'TContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> = {
    /// used to make processing idempotent
    GetUniqueId : 'TMetadata -> string option
    StreamMetadata : EventStreamMetadata
    GetStreamName : 'TContext -> 'TAggregateId -> string
    StateBuilder : IStateBuilder<Map<string,obj>, 'TMetadata, unit>
    StateChangeHandlers : LazyList<IStateChangeHandler<'TMetadata, 'TBaseEvent>>
}

type ICommandHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, unit> list -> IStateBlockBuilder<'TMetadata, unit> list
    abstract member GetId : 'TCommandContext-> obj -> 'TAggregateId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> -> 'TCommandContext -> obj -> EventStreamProgram<CommandResult<'TBaseEvent,'TMetadata>,'TMetadata>
    abstract member Visitable : IRegistrationVisitable

type IEventHandler<'TAggregateId,'TMetadata, 'TEventContext,'TBaseEvent when 'TAggregateId : equality> =
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, unit> list -> IStateBlockBuilder<'TMetadata, unit> list
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TEventContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> -> 'TEventContext -> PersistedEvent<'TMetadata> -> Async<EventStreamProgram<EventResult,'TMetadata>>

type IMultiCommandEventHandler<'TMetadata, 'TEventContext,'TCommandContext, 'TBaseEvent> = 
     abstract member EventType : Type
     abstract member Handler : 'TEventContext -> PersistedEvent<'TMetadata> -> MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseEvent,'TMetadata>>

type IWakeupHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> =
    abstract member WakeupFold : WakeupFold<'TMetadata>
                            //streamId -> getUniqueId                   -> time     -> program
    abstract member Handler : AggregateConfiguration<'TEventContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> -> string   -> UtcDateTime -> EventStreamProgram<EventResult,'TMetadata>

type AggregateCommandHandlers<'TAggregateId,'TCommandContext,'TMetadata, 'TBaseEvent> = seq<ICommandHandler<'TAggregateId,'TCommandContext,'TMetadata, 'TBaseEvent>>
type AggregateEventHandlers<'TAggregateId,'TMetadata, 'TEventContext,'TBaseEvent  when 'TAggregateId : equality> = seq<IEventHandler<'TAggregateId, 'TMetadata, 'TEventContext,'TBaseEvent >>

type AggregateHandlerState<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent when 'TId : equality> = {
    commandHandlers : list<ICommandHandler<'TId,'TCommandContext,'TMetadata, 'TBaseEvent>> 
    eventHandlers : list<IEventHandler<'TId,'TMetadata, 'TEventContext,'TBaseEvent>>
    stateChangeHandlers : list<IStateChangeHandler<'TMetadata, 'TBaseEvent>>
    multiCommandEventHandlers : list<IMultiCommandEventHandler<'TMetadata, 'TEventContext,'TCommandContext, 'TBaseEvent>>
}
with 
    static member Empty = 
        { commandHandlers = List.empty
          eventHandlers = List.empty
          stateChangeHandlers = List.empty
          multiCommandEventHandlers = List.empty }

type AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent when 'TId : equality> private 
    (
        state : AggregateHandlerState<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent> 
    ) =
    member x.CommandHandlers = state.commandHandlers
    member x.EventHandlers = state.eventHandlers
    member x.StateChangeHandlers = state.stateChangeHandlers
    member x.MultiCommandEventHandlers = state.multiCommandEventHandlers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with commandHandlers = handler::state.commandHandlers})
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with eventHandlers = handler::state.eventHandlers})
    member x.AddStateChangeHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with stateChangeHandlers = handler::state.stateChangeHandlers})
    member x.AddMultiCommandEventHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with multiCommandEventHandlers = handler::state.multiCommandEventHandlers})
    member x.Combine (y:AggregateHandlers<_,_,_,_,_>) =
        new AggregateHandlers<_,_,_,_,_>(
            { 
                commandHandlers = List.append state.commandHandlers y.CommandHandlers
                eventHandlers = List.append state.eventHandlers y.EventHandlers
                stateChangeHandlers = List.append state.stateChangeHandlers y.StateChangeHandlers
                multiCommandEventHandlers = List.append state.multiCommandEventHandlers y.MultiCommandEventHandlers
            })

    static member Empty =
        let state = 
            { commandHandlers = List.empty
              eventHandlers = List.empty
              stateChangeHandlers = List.empty
              multiCommandEventHandlers = List.empty } 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>(state)

module AggregateHandlers =
    let addCommandHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddCommandHandler handler

    let addCommandHandlers handlers (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        handlers
        |> Seq.fold (fun (s:AggregateHandlers<_,_,_,_,_>) h -> s.AddCommandHandler h) aggregateHandlers

    let addEventHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddEventHandler handler

    let addEventHandlers handlers (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        handlers
        |> Seq.fold (fun (s:AggregateHandlers<_,_,_,_,_>) h -> s.AddEventHandler h) aggregateHandlers

    let addStateChangeHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddStateChangeHandler handler

    let addMultiCommandEventHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddMultiCommandEventHandler handler

type IHandler<'TId,'TCommandContext,'TEventContext when 'TId : equality> = 
    abstract member add : AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent> -> AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>

open FSharpx
open Eventful.Validation

type CommandHandler<'TCmd, 'TCommandContext, 'TCommandState, 'TAggregateId, 'TMetadata, 'TBaseEvent> = {
    GetId : 'TCommandContext -> 'TCmd -> 'TAggregateId
    StateBuilder : IStateBuilder<'TCommandState, 'TMetadata, unit>
    Handler : 'TCommandState -> 'TCommandContext -> 'TCmd -> Async<Choice<seq<'TBaseEvent * 'TMetadata>, NonEmptyList<ValidationFailure>>>
}

type MultiEventRun<'TAggregateId,'TMetadata,'TState,'TBaseEvent when 'TAggregateId : equality> = ('TAggregateId * ('TState -> seq<'TBaseEvent * 'TMetadata>))

open Eventful.Validation

module AggregateActionBuilder =
    open EventStream

    let log = createLogger "Eventful.AggregateActionBuilder"

    let fullHandlerAsync<'TId, 'TState,'TCmd, 'TCommandContext,'TMetadata, 'TBaseEvent,'TKey> getId (stateBuilder : IStateBuilder<_,_,'TKey>) f =
        {
            GetId = getId
            StateBuilder = StateBuilder.withUnitKey stateBuilder
            Handler = f
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent> 

    let fullHandler<'TId, 'TState,'TCmd,'TCommandContext,'TMetadata, 'TBaseEvent,'TKey> getId (stateBuilder : IStateBuilder<_,_,'TKey>) f =
        {
            GetId = getId
            StateBuilder = StateBuilder.withUnitKey stateBuilder
            Handler = (fun a b c ->  f a b c |> Async.returnM)
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent> 

    let simpleHandler<'TAggregateId, 'TState, 'TCmd, 'TCommandContext, 'TMetadata, 'TBaseEvent> getId stateBuilder (f : 'TCmd -> ('TBaseEvent * 'TMetadata)) =
        {
            GetId = getId
            StateBuilder = stateBuilder
            Handler = (fun _ _ -> f >> Seq.singleton >> Success >> Async.returnM)
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TAggregateId, 'TMetadata, 'TBaseEvent> 

    let toChoiceValidator cmd r =
        if r |> Seq.isEmpty then
            Success cmd
        else
            NonEmptyList.create (r |> Seq.head) (r |> Seq.tail |> List.ofSeq) |> Failure

    let untypedGetId<'TId,'TCmd,'TState, 'TCommandContext, 'TMetadata, 'TValidatedState, 'TBaseEvent> (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) (context : 'TCommandContext) (cmd:obj) =
        match cmd with
        | :? 'TCmd as cmd ->
            sb.GetId context cmd
        | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())

    let uniqueIdBuilder getUniqueId =
        StateBuilder.Empty "$Eventful::UniqueIdBuilder" Set.empty
        |> StateBuilder.allEventsHandler 
            (fun m -> ()) 
            (fun (s,e,m) -> 
                match getUniqueId m with
                | Some uniqueId ->
                    s |> Set.add uniqueId
                | None -> s)
        |> StateBuilder.toInterface

    let writeEvents stream lastEventNumber streamMetadata events =  eventStream {
        let rawEventToEventStreamEvent (event, metadata) = getEventStreamEvent event metadata
        let! eventStreamEvents = EventStream.mapM rawEventToEventStreamEvent events

        let expectedVersion = 
            match lastEventNumber with
            | -1 -> NewStream
            | x -> AggregateVersion x

        if streamMetadata <> EventStreamMetadata.Default then
            if expectedVersion = NewStream then
                do! writeStreamMetadata stream streamMetadata

        return! writeToStream stream expectedVersion eventStreamEvents
    }

    let addStateChangeEvents blockBuilders snapshot stateChangeHandlers events = 
        let applyEventToSnapshot (event, metadata) snapshot = 
            AggregateStateBuilder.applyToSnapshot blockBuilders () event (snapshot.LastEventNumber + 1) metadata snapshot

        let runStateChangeHandlers beforeSnapshot afterSnapshot =
            LazyList.map (fun (h:IStateChangeHandler<_, _>) -> h.Handler beforeSnapshot afterSnapshot |> LazyList.ofSeq)
            >> LazyList.concat
        
        let generator (beforeSnapshot, events) = maybe {
            let! (x,xs) = LazyList.tryUncons events
            let afterSnapshot = applyEventToSnapshot x beforeSnapshot
            let remainingEvents = 
                stateChangeHandlers
                |> runStateChangeHandlers beforeSnapshot afterSnapshot

            return 
                (x, (afterSnapshot, LazyList.append remainingEvents xs))
        }
        
        LazyList.unfold generator (snapshot, events)

    let anyNewUniqueIdsAlreadyExist getUniqueId snapshot events =
        let existingUniqueIds = (uniqueIdBuilder getUniqueId).GetState snapshot.State
        let newUniqueIds = 
            events
            |> Seq.map snd
            |> Seq.map getUniqueId
            |> Seq.collect (Option.toList)
            |> Set.ofSeq

        existingUniqueIds |> Set.intersect newUniqueIds |> Set.isEmpty |> not
        
    let inline runHandler 
        getUniqueId
        streamMetadata
        blockBuilders
        stateChangeHandlers
        streamId 
        snapshot
        (commandStateBuilder : IStateBuilder<'TChildState, 'TMetadata, unit>) 
        f = 

        eventStream {
            let commandState = commandStateBuilder.GetState snapshot.State

            let! result = runAsync <| f commandState

            return! 
                match result with
                | Choice1Of2 (handlerEvents : seq<'TBaseEvent * 'TMetadata>) -> 
                    eventStream {
                        if (anyNewUniqueIdsAlreadyExist getUniqueId snapshot handlerEvents) then
                            return Choice2Of2 RunFailure<_>.AlreadyProcessed
                        else
                            let events = 
                                handlerEvents
                                |> LazyList.ofSeq
                                |> addStateChangeEvents blockBuilders snapshot stateChangeHandlers
                                |> List.ofSeq

                            let! writeResult = writeEvents streamId snapshot.LastEventNumber streamMetadata events

                            log.Debug <| lazy (sprintf "WriteResult: %A" writeResult)
                            
                            return 
                                match writeResult with
                                | WriteResult.WriteSuccess pos ->
                                    Choice1Of2 {
                                        Events = events |> List.map (fun (a,b) -> (streamId, a, b))
                                        Position = Some pos
                                    }
                                | WriteResult.WrongExpectedVersion -> 
                                    Choice2Of2 RunFailure<_>.WrongExpectedVersion
                                | WriteResult.WriteError ex -> 
                                    Choice2Of2 <| RunFailure<_>.WriteError ex
                                | WriteResult.WriteCancelled -> 
                                    Choice2Of2 RunFailure<_>.WriteCancelled
                    }
                | Choice2Of2 x ->
                    eventStream { 
                        return
                            HandlerError x
                            |> Choice2Of2
                    }
        }

    let mapValidationFailureToCommandFailure (x : RunFailure<_>) =
        match x with
        | HandlerError x -> 
            (NonEmptyList.map CommandFailure.ofValidationFailure) x
        | RunFailure.WrongExpectedVersion ->
            CommandFailure.CommandError "WrongExpectedVersion"
            |> NonEmptyList.singleton 
        | AlreadyProcessed ->
            CommandFailure.CommandError "AlreadyProcessed"
            |> NonEmptyList.singleton 
        | RunFailure.WriteCancelled ->
            CommandFailure.CommandError "WriteCancelled"
            |> NonEmptyList.singleton 
        | RunFailure.WriteError ex 
        | RunFailure.Exception ex ->
            CommandFailure.CommandException (None, ex)
            |> NonEmptyList.singleton 

    let retryOnWrongVersion streamId stateBuilder handler = 
        let program attempt = eventStream {
            log.RichDebug "Starting attempt {@Attempt}" [|attempt|]
            let! streamState = 
                    stateBuilder
                    |> AggregateStateBuilder.toStreamProgram streamId ()

            return! handler streamState
        }
        EventStream.retryOnWrongVersion program

    let handleCommand
        (commandHandler:CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) 
        (aggregateConfiguration : AggregateConfiguration<'TCommandContext,_,_,_>) 
        (commandContext : 'TCommandContext) 
        (cmd : obj)
        : EventStreamProgram<CommandResult<'TBaseEvent,'TMetadata>,'TMetadata> =
        let processCommand cmd commandState = async {
            return! commandHandler.Handler commandState commandContext cmd
        }

        match cmd with
        | :? 'TCmd as cmd -> 
            let getId = FSharpx.Choice.protect (commandHandler.GetId commandContext) cmd
            match getId with
            | Choice1Of2 aggregateId ->
                let stream = aggregateConfiguration.GetStreamName commandContext aggregateId

                let f snapshot = 
                   runHandler 
                        aggregateConfiguration.GetUniqueId 
                        aggregateConfiguration.StreamMetadata
                        aggregateConfiguration.StateBuilder.GetBlockBuilders
                        aggregateConfiguration.StateChangeHandlers
                        stream 
                        snapshot
                        commandHandler.StateBuilder 
                        (processCommand cmd) 

                eventStream {
                    do! logMessage LogMessageLevel.Debug "Starting command tries" [||]
                    let! result = retryOnWrongVersion stream aggregateConfiguration.StateBuilder f
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
        
    let ToInterface (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TAggregateId, 'TMetadata,'TBaseEvent>) = 
        let cmdType = typeof<'TCmd>
        if cmdType = typeof<obj> then
            failwith "Command handler registered for type object. You might need to specify a type explicitely."
        else
            { new ICommandHandler<'TAggregateId,'TCommandContext,'TMetadata, 'TBaseEvent> with 
                 member this.GetId context cmd = untypedGetId sb context cmd
                 member this.CmdType = cmdType
                 member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers sb.StateBuilder.GetBlockBuilders builders
                 member this.Handler (aggregateConfig : AggregateConfiguration<'TCommandContext, 'TAggregateId, 'TMetadata, 'TBaseEvent>) commandContext cmd = 
                    handleCommand sb aggregateConfig commandContext cmd
                 member this.Visitable = {
                    new IRegistrationVisitable with
                        member x.Receive a r = r.Visit<'TCmd>(a)
                 }
            }

    let withCmdId (getId : 'TCmd -> 'TId) (builder : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) = 
        { builder with GetId = (fun _ cmd -> getId cmd )}

    let buildCmd (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) : ICommandHandler<'TId,'TCommandContext,'TMetadata, 'TBaseEvent> = 
        ToInterface sb

    let buildStateChange (stateBuilder : IStateBuilder<'TState,'TMetadata,unit>) (handler : 'TState -> 'TState -> seq<'TBaseEvent * 'TMetadata>) = {
        new IStateChangeHandler<'TMetadata, 'TBaseEvent> with
            member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers stateBuilder.GetBlockBuilders builders
            member this.Handler beforeSnapshot afterSnapshot =
                let beforeState = stateBuilder.GetState beforeSnapshot.State
                let afterState = stateBuilder.GetState afterSnapshot.State
                if beforeState <> afterState then
                    handler beforeState afterState
                else
                    Seq.empty
    }

    let getEventInterfaceForLink<'TLinkEvent,'TAggregateId,'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent when 'TAggregateId : equality> (fId : 'TLinkEvent -> 'TAggregateId) (metadataBuilder : metadataBuilder<'TMetadata>) = {
        new IEventHandler<'TAggregateId,'TMetadata, 'TEventContext,'TBaseEvent> with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder builders = builders
             member this.Handler aggregateConfig eventContext (evt : PersistedEvent<'TMetadata>) = 
                eventStream {
                    let aggregateId = fId (evt.Body :?> 'TLinkEvent)
                    let sourceMessageId = evt.EventId.ToString()
                    let metadata = metadataBuilder sourceMessageId

                    let resultingStream = aggregateConfig.GetStreamName eventContext aggregateId

                    let! result = EventStream.writeLink resultingStream Any evt.StreamId evt.EventNumber metadata

                    return 
                        match result with
                        | WriteResult.WriteSuccess _ -> 
                            log.Debug <| lazy (sprintf "Wrote Link To %A %A %A" DateTime.Now.Ticks evt.StreamId evt.EventNumber)
                            ()
                        | WriteResult.WrongExpectedVersion -> failwith "WrongExpectedVersion writing event. TODO: retry"
                        | WriteResult.WriteError ex -> failwith <| sprintf "WriteError writing event: %A" ex
                        | WriteResult.WriteCancelled -> failwith "WriteCancelled writing event" } 
                |> Async.returnM
    } 

    let processSequence
        (aggregateConfiguration : AggregateConfiguration<'TEventContext,'TId,'TMetadata, 'TBaseEvent>)
        (stateBuilder : IStateBuilder<'TState,_,_>)
        (event: PersistedEvent<'TMetadata>)
        (eventContext : 'TEventContext)  
        (handlers : AsyncSeq<MultiEventRun<'TId,'TMetadata,'TState,'TBaseEvent>>) 
        =
        handlers
        |> AsyncSeq.map (fun (id, handler) ->
            eventStream {
                let resultingStream = aggregateConfiguration.GetStreamName eventContext id
                let f snapshot = 
                    let stateBuilder = stateBuilder |> StateBuilder.withUnitKey
                    runHandler 
                        aggregateConfiguration.GetUniqueId 
                        aggregateConfiguration.StreamMetadata
                        aggregateConfiguration.StateBuilder.GetBlockBuilders
                        aggregateConfiguration.StateChangeHandlers
                        resultingStream 
                        snapshot
                        stateBuilder 
                        (handler >> Choice1Of2 >> Async.returnM) 

                let! result = retryOnWrongVersion resultingStream aggregateConfiguration.StateBuilder f

                return 
                    match result with
                    | Choice1Of2 _ -> ()
                    | Choice2Of2 AlreadyProcessed -> ()
                    | Choice2Of2 a ->
                        failwith <| sprintf "Event handler failed: %A" a
            }
        )

    let getEventInterfaceForOnEvent<'TOnEvent, 'TId, 'TState, 'TMetadata, 'TCommandContext, 'TEventContext,'TBaseEvent,'TKey when 'TId : equality> (stateBuilder: IStateBuilder<_,_,'TKey>) (fId : ('TOnEvent * 'TEventContext) -> AsyncSeq<MultiEventRun<'TId,'TMetadata,'TState,'TBaseEvent>>) = 
        let evtType = typeof<'TOnEvent>
        if evtType = typeof<obj> then
            failwith "Event handler registered for type object. You might need to specify a type explicitely."
        else 
            {
                new IEventHandler<'TId,'TMetadata, 'TEventContext,'TBaseEvent> with 
                    member this.EventType = typeof<'TOnEvent>
                    member this.AddStateBuilder builders = 
                        let stateBuilderBlockBuilders = 
                            stateBuilder
                            |> StateBuilder.withUnitKey
                            |> (fun x -> x.GetBlockBuilders)
                        AggregateStateBuilder.combineHandlers stateBuilderBlockBuilders builders
                    member this.Handler aggregateConfig eventContext evt = 
                        let typedEvent = evt.Body :?> 'TOnEvent

                        fId (typedEvent, eventContext) 
                        |> processSequence aggregateConfig stateBuilder evt eventContext
                        |> AsyncSeq.fold EventStream.combine EventStream.empty
            }

    let linkEvent<'TLinkEvent,'TId,'TCommandContext,'TEventContext,'TMetadata, 'TBaseEvent when 'TId : equality> fId (metadata : metadataBuilder<'TMetadata>) = 
        getEventInterfaceForLink<'TLinkEvent,'TId,'TMetadata,'TCommandContext,'TEventContext, 'TBaseEvent> fId metadata

    let onEventMultiAsync<'TOnEvent,'TEventState,'TId, 'TMetadata, 'TCommandContext,'TEventContext,'TBaseEvent,'TKey when 'TId : equality> 
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TKey>)  
        (fId : ('TOnEvent * 'TEventContext) -> AsyncSeq<MultiEventRun<'TId,'TMetadata,'TEventState,'TBaseEvent>>) = 
        getEventInterfaceForOnEvent<'TOnEvent,'TId,'TEventState, 'TMetadata, 'TCommandContext,'TEventContext,'TBaseEvent,'TKey> stateBuilder fId

    let onEventMulti<'TOnEvent,'TEventState,'TId, 'TMetadata, 'TCommandContext,'TEventContext,'TBaseEvent,'TKey when 'TId : equality> 
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TKey>)  
        (fId : ('TOnEvent * 'TEventContext) -> seq<MultiEventRun<'TId,'TMetadata,'TEventState,'TBaseEvent>>) = 
        onEventMultiAsync stateBuilder (fId >> AsyncSeq.ofSeq)

    let onEvent<'TOnEvent,'TEventState,'TId,'TMetadata,'TEventContext,'TBaseEvent, 'TKey when 'TId : equality> 
        (fId : 'TOnEvent -> 'TEventContext -> 'TId) 
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TKey>) 
        (runEvent : 'TEventState -> 'TOnEvent -> 'TEventContext -> seq<'TBaseEvent * 'TMetadata>) = 
        
        let runEvent' = fun evt eventCtx state ->
            runEvent state evt eventCtx

        let handler = 
            (fun (evt, eventCtx) -> 
                let aggId = fId evt eventCtx
                let h = runEvent' evt eventCtx
                (aggId, h) |> Seq.singleton
            )
        onEventMulti stateBuilder handler

    let multiCommandEventHandler (f : 'TOnEvent -> 'TEventContext -> MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseEvent,'TMetadata>>) =
        {
            new IMultiCommandEventHandler<'TMetadata, 'TEventContext,'TCommandContext, 'TBaseEvent> with 
                member this.EventType = typeof<'TOnEvent>
                member this.Handler eventContext evt =
                    let typedEvent = evt.Body :?> 'TOnEvent

                    f typedEvent eventContext
        }

type AggregateDefinition<'TAggregateId, 'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent when 'TAggregateId : equality> = {
    GetUniqueId : 'TMetadata -> string option
    GetCommandStreamName : 'TCommandContext -> 'TAggregateId -> string
    GetEventStreamName : 'TEventContext -> 'TAggregateId -> string
    Handlers : AggregateHandlers<'TAggregateId, 'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent>
    AggregateType : string
    Wakeup : IWakeupHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> option
    StreamMetadata : EventStreamMetadata
    ExtraStateBuilders : IStateBlockBuilder<'TMetadata, unit> list
}

module Aggregate = 
    let aggregateDefinitionFromHandlers
        (aggregateType : string)
        (getUniqueId : 'TMetadata -> string option)
        (getCommandStreamName : 'TCommandContext -> 'TAggregateId -> string)
        (getEventStreamName : 'TEventContext -> 'TAggregateId -> string) 
        handlers =
            {
                AggregateType = aggregateType
                GetUniqueId = getUniqueId
                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
                Handlers = handlers
                Wakeup = None
                StreamMetadata = EventStreamMetadata.Default
                ExtraStateBuilders = []
            }

    let toAggregateDefinition<'TEvents, 'TAggregateId, 'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent when 'TAggregateId : equality>
        (aggregateType : string)
        (getUniqueId : 'TMetadata -> string option)
        (getCommandStreamName : 'TCommandContext -> 'TAggregateId -> string)
        (getEventStreamName : 'TEventContext -> 'TAggregateId -> string) 
        (commandHandlers : AggregateCommandHandlers<'TAggregateId,'TCommandContext, 'TMetadata,'TBaseEvent>)
        (eventHandlers : AggregateEventHandlers<'TAggregateId,'TMetadata, 'TEventContext, 'TBaseEvent>)
        = 
            let handlers =
                commandHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddCommandHandler h) AggregateHandlers.Empty

            let handlers =
                eventHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddEventHandler h) handlers

            aggregateDefinitionFromHandlers 
                aggregateType
                getUniqueId
                getCommandStreamName
                getEventStreamName
                handlers

    let withExtraStateBuilders
        (extraStateBuilders : IStateBlockBuilder<'TMetadata, unit> list) 
        (aggregateDefinition : AggregateDefinition<_,_,_,'TMetadata,'TBaseEvent>) =

        { aggregateDefinition with ExtraStateBuilders = extraStateBuilders }

    let withWakeup 
        (wakeupFold : WakeupFold<'TMetadata>) 
        (stateBuilder : IStateBuilder<'T,'TMetadata, unit>) 
        (wakeupHandler : UtcDateTime -> 'T ->  seq<'TBaseEvent * 'TMetadata>)
        (aggregateDefinition : AggregateDefinition<_,_,_,'TMetadata,'TBaseEvent>) =

        let handler time t : Async<Choice<seq<'TBaseEvent * 'TMetadata>,_>> =
            wakeupHandler time t 
            |> Choice1Of2
            |> Async.returnM

        let wakeup = {
            new IWakeupHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> with
                member x.WakeupFold = wakeupFold
                member x.Handler aggregateConfiguration streamId (time : UtcDateTime) =
                    eventStream {
                        let run streamState = 
                            // ensure that the handler is only run if the state matches the time
                            let nextWakeupTimeFromState = wakeupFold.GetState streamState.State
                            if Some time = nextWakeupTimeFromState then
                                AggregateActionBuilder.runHandler 
                                    aggregateConfiguration.GetUniqueId 
                                    aggregateConfiguration.StreamMetadata 
                                    aggregateConfiguration.StateBuilder.GetBlockBuilders 
                                    aggregateConfiguration.StateChangeHandlers 
                                    streamId 
                                    streamState 
                                    stateBuilder 
                                    (handler time)
                            else
                                // if the time is out of date just return success with no events
                                eventStream {
                                    do! logMessage LogMessageLevel.Debug "Ignoring out of date wakeup time {@NextExpectedWakeupTime} {@WakeupTime}" [|nextWakeupTimeFromState;time|]
                                    return
                                        CommandSuccess<'TBaseEvent,'TMetadata>.Empty
                                        |> Choice1Of2
                                }
                        let! result = AggregateActionBuilder.retryOnWrongVersion streamId aggregateConfiguration.StateBuilder run
                        match result with
                        | Choice2Of2 failure ->
                            failwith <| sprintf "WakeupHandler failed %A" failure
                        | _ -> 
                            ()
                    }
        }
        { aggregateDefinition with Wakeup = Some wakeup }

    let withStreamMetadata 
        (streamMetadata : EventStreamMetadata) 
        (aggregateDefinition : AggregateDefinition<_,_,_,'TMetadata,'TBaseEvent>) =
        { aggregateDefinition with StreamMetadata = streamMetadata } 