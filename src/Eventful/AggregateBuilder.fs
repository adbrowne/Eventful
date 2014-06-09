namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

type EventMetadata = {
    MessageId : Guid
    SourceMessageId : Guid
}

type CommandResult = Choice<list<string * obj * EventMetadata>,NonEmptyList<ValidationFailure>> 

type ICommandHandler<'TState,'TEvent,'TId when 'TId :> IIdentity> =
    abstract member CmdType : Type
    abstract member GetId : obj -> 'TId
//    abstract member StateValidation : 'TState option -> seq<ValidationFailure>
//    abstract member CommandValidation : 'TCmd -> seq<ValidationFailure>
    abstract member Handler : 'TState option -> obj -> Choice<seq<'TEvent>,NonEmptyList<ValidationFailure>>

type IEventHandler<'TState,'TEvent,'TId> =
    abstract member CmdType : Type
    abstract member Handler : 'TState option -> obj -> Choice<seq<'TEvent>,seq<ValidationFailure>>

type IEventLinker<'TEvent,'TId> =
    abstract member EventType : Type
    abstract member GetId : obj -> 'TId

type AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity> private 
    (
        aggregateType : 'TAggregateType,
        commandHandlers : list<ICommandHandler<'TState,'TEvent,'TId>>, 
        eventHandlers : list<IEventHandler<'TState,'TEvent,'TId>>,
        eventLinkers  : list<IEventLinker<'TEvent,'TId>>,
        stateBuilder : StateBuilder<'TState>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventLinkers = eventLinkers
    member x.AggregateType = aggregateType
    member x.StateBuilder = stateBuilder
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, handler::commandHandlers, eventHandlers, eventLinkers, stateBuilder)
    member x.AddEventLinker linker = 
        new AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, commandHandlers, eventHandlers, linker::eventLinkers, stateBuilder)
    member x.Combine (y:AggregateHandlers<_,_,_,_>) =
        new AggregateHandlers<_,_,_,_>(
            aggregateType,
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers, 
            List.append eventLinkers y.EventLinkers,
            stateBuilder)

    static member Empty aggregateType stateBuilder = new AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, List.empty, List.empty, List.empty, stateBuilder)
    
type IHandler<'TState,'TEvent,'TId when 'TId :> IIdentity> = 
    abstract member add : AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType> -> AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>

open FSharpx
open Eventful.Validation

type Validator<'TCmd,'TState> = 
| CommandValidator of ('TCmd -> seq<ValidationFailure>)
| StateValidator of ('TState option -> seq<ValidationFailure>)
| CombinedValidator of ('TCmd -> 'TState option -> seq<ValidationFailure>)

type CommandHandler<'TCmd, 'TState, 'TId, 'TEvent when 'TId :> IIdentity> = {
    GetId : 'TCmd -> 'TId
    Validators : Validator<'TCmd,'TState> list
    Handler : 'TCmd -> seq<'TEvent>
}

open Eventful.Validation

module AggregateActionBuilder =
    let simpleHandler<'TId,'TCmd,'TEvent,'TState when 'TId :> IIdentity> (f : 'TCmd -> 'TEvent) =
        {
            GetId = MagicMapper.magicId<'TId>
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

    let ToInterface<'TId,'TCmd,'TEvent,'TState when 'TId :> IIdentity> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) = {
            new ICommandHandler<'TState,'TEvent,'TId> with 
                 member this.GetId cmd = untypedGetId sb cmd
                 member this.CmdType = typeof<'TCmd>
                 member this.Handler state cmd =
                    choose {
                        match cmd with
                        | :? 'TCmd as cmd -> 
                            let! validated = runValidation sb.Validators cmd state

                            return sb.Handler validated
                        | _ -> return! Choice2Of2 <| NonEmptyList.singleton (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>)
                    }
        }

    let buildCmd<'TId,'TCmd,'TEvent,'TState when 'TId :> IIdentity> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) : IHandler<'TState,'TEvent,'TId> = {
            new IHandler<'TState,'TEvent,'TId> with
                member x.add handlers =
                    let cmdInterface = ToInterface sb
                    handlers.AddCommandHandler cmdInterface
        }

    let addValidator 
        (validator : Validator<'TCmd,'TState>) 
        (handler: CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) = 
        { handler with Validators = validator::handler.Validators }

    let ensureFirstCommand x = addValidator (StateValidator (isNone id "Must be the first command")) x

    let buildSimpleCmdHandler<'TId,'TCmd,'TEvent,'TState when 'TId :> IIdentity> = 
        simpleHandler<'TId,'TCmd,'TEvent,'TState> >> buildCmd

    let getLinkerInterface<'TLinkEvent,'TEvent,'TId> fId : IEventLinker<'TEvent,'TId> = {
        new IEventLinker<'TEvent,'TId> with
            member x.EventType = typeof<'TLinkEvent>
            member x.GetId (event : obj) = 
                match event with
                | :? 'TLinkEvent as event -> fId event
                | _ -> failwith (sprintf "Expecting event of type: %A received %A" typeof<'TLinkEvent> (event.GetType()))
    }
        
    let linkEvent<'TLinkEvent,'TEvent,'TId,'TState when 'TId :> IIdentity> fId (linkEvent : 'TLinkEvent -> 'TEvent) = {
        new IHandler<'TState,'TEvent,'TId> with
            member x.add handlers =
                let linkerInterface = (getLinkerInterface<'TLinkEvent,'TEvent,'TId> fId)
                handlers.AddEventLinker linkerInterface
    }

module Aggregate = 
    type AggregateBuilder<'TState,'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity> (aggregateType : 'TAggregateType, stateBuilder : StateBuilder<'TState>) = 
        member this.Zero() = AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>.Empty

        member x.Delay(f : unit -> AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>) = f ()

        member this.Yield(x:IHandler<'TState,'TEvent,'TId>) :  AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType> =
            let empty = AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>.Empty aggregateType stateBuilder
            let result = x.add empty
            result

        member this.Combine (a:AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>,b:AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>) =
            a.Combine b

    let aggregate<'TState,'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity> aggregateType stateBuilder = 
        new AggregateBuilder<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, stateBuilder)