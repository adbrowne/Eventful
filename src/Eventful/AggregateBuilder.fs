namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

type ValidationFailure = string

type EventMetadata = {
    MessageId : Guid
    SourceMessageId : Guid
}

type CommandResult = Choice<list<string * obj * EventMetadata>,NonEmptyList<ValidationFailure>> 

type ICommandHandler<'TState,'TEvent,'TId when 'TId :> IIdentity> =
    abstract member CmdType : Type
    abstract member GetId : obj -> 'TId
    abstract member StateValidation : 'TState option -> seq<ValidationFailure>
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
        eventLinkers  : list<IEventLinker<'TEvent,'TId>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventLinkers = eventLinkers
    member x.AggregateType = aggregateType
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, handler::commandHandlers, eventHandlers, eventLinkers)
    member x.AddEventLinker linker = 
        new AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, commandHandlers, eventHandlers, linker::eventLinkers)
    member x.Combine (y:AggregateHandlers<_,_,_,_>) =
        new AggregateHandlers<_,_,_,_>(
            aggregateType,
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers, 
            List.append eventLinkers y.EventLinkers)

    static member Empty aggregateType = new AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType, List.empty, List.empty, List.empty)
    
type IHandler<'TState,'TEvent,'TId when 'TId :> IIdentity> = 
    abstract member add : AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType> -> AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>

type CommandHandler<'TCmd, 'TState, 'TId, 'TEvent when 'TId :> IIdentity> = {
    GetId : 'TCmd -> 'TId
    StateValidation : 'TState option -> seq<ValidationFailure>
    Validate : 'TCmd -> 'TState option -> Choice<'TCmd,NonEmptyList<ValidationFailure>>
    Handler : 'TCmd -> seq<'TEvent>
}
 with
    static member ToInterface<'TState,'TEvent> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) = {
            new ICommandHandler<'TState,'TEvent,'TId> with 
                 member this.GetId cmd = 
                    match cmd with
                    | :? 'TCmd as cmd ->
                        sb.GetId cmd
                    | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())
                 member this.CmdType = typeof<'TCmd>
                 member this.StateValidation state = sb.StateValidation state
                 member this.Handler state cmd =
                    choose {
                        match cmd with
                        | :? 'TCmd as cmd -> 
                            let! validated = sb.Validate cmd state
                            return sb.Handler validated
                        | _ -> return! Choice2Of2 <| NonEmptyList.singleton (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>)
                    }
        }
    static member ToAdded<'TState,'TEvent,'TId> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) : IHandler<'TState,'TEvent,'TId> = {
            new IHandler<'TState,'TEvent,'TId> with
                member x.add handlers =
                    let cmdInterface = CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>.ToInterface sb
                    handlers.AddCommandHandler cmdInterface
        }

module AggregateActionBuilder =
    let simpleHandler<'TId,'TCmd,'TEvent,'TState when 'TId :> IIdentity> (f : 'TCmd -> 'TEvent) =
        {
            GetId = MagicMapper.magicId<'TId>
            StateValidation = (fun _ -> Seq.empty)
            Validate = (fun cmd _ -> Choice1Of2 cmd)
            Handler = f >> Seq.singleton
        } : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent> 

    let buildCmd (handler: CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>) = CommandHandler<'TCmd, 'TState, 'TId, 'TEvent>.ToAdded handler

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

module Aggregate2 = 
    type AggregateBuilder<'TState,'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity> (aggregateType : 'TAggregateType) = 
        member this.Zero() = AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>.Empty

        member x.Delay(f : unit -> AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>) = f ()

        member this.Yield(x:IHandler<'TState,'TEvent,'TId>) :  AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType> =
            let empty = AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>.Empty aggregateType
            let result = x.add empty
            result
        member this.Combine (a:AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>,b:AggregateHandlers<'TState,'TEvent,'TId, 'TAggregateType>) =
            a.Combine b

//        [<CustomOperation("ldc_i4", MaintainsVariableSpace=true)>]
//        member __.Ldc_I4((Instrs f : Instrs<'a,'r,_>, j), [<ProjectionParameter>]h:_->int) : Instrs<V<int> * 'a,'r,Nok> * _ =
//            Instrs(f +> fun s -> s.ilg.Emit(OpCodes.Ldc_I4, h j)), j

    let aggregate<'TState,'TEvent,'TId, 'TAggregateType when 'TId :> IIdentity> aggregateType = 
        new AggregateBuilder<'TState,'TEvent,'TId, 'TAggregateType>(aggregateType : 'TAggregateType)