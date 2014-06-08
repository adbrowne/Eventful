namespace Eventful

open System
open FSharpx.Choice

type ValidationFailure = string

type EventMetadata = {
    MessageId : Guid
    SourceMessageId : Guid
}

type ICommandHandler<'TState,'TEvent,'TId> =
    abstract member CmdType : Type
    abstract member GetId : obj -> 'TId
    abstract member Handler : 'TState option -> obj -> Choice<seq<'TEvent>,seq<ValidationFailure>>

type IEventHandler<'TState,'TEvent,'TId> =
    abstract member CmdType : Type
    abstract member Handler : 'TState option -> obj -> Choice<seq<'TEvent>,seq<ValidationFailure>>

type IEventLinker<'TEvent,'TId> =
    abstract member EventType : Type
    abstract member GetId : obj -> 'TId

type AggregateHandlers<'TState,'TEvent,'TId> private 
    (
        commandHandlers : list<ICommandHandler<'TState,'TEvent,'TId>>, 
        eventHandlers : list<IEventHandler<'TState,'TEvent,'TId>>,
        eventLinkers  : list<IEventLinker<'TEvent,'TId>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.EventLinkers = eventLinkers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TState,'TEvent,'TId>(handler::commandHandlers, eventHandlers, eventLinkers)
    member x.AddEventLinker linker = 
        new AggregateHandlers<'TState,'TEvent,'TId>(commandHandlers, eventHandlers, linker::eventLinkers)
    member x.Combine (y:AggregateHandlers<_,_,_>) =
        new AggregateHandlers<_,_,_>(
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers, 
            List.append eventLinkers y.EventLinkers)

    static member Empty = new AggregateHandlers<'TState,'TEvent,'TId>(List.empty, List.empty, List.empty)
    
type IHandler<'TState,'TEvent,'TId> = 
    abstract member add : AggregateHandlers<'TState,'TEvent,'TId> -> AggregateHandlers<'TState,'TEvent,'TId>

type CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TValidatedCmd> = {
    GetId : 'TCmd -> 'TId
    Validate : 'TCmd -> 'TState option -> Choice<'TValidatedCmd,seq<ValidationFailure>>
    Handler : 'TValidatedCmd -> seq<'TEvent>
}
 with
    static member ToInterface<'TState,'TEvent> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TValidatedCmd>) = {
            new ICommandHandler<'TState,'TEvent,'TId> with 
                 member this.GetId cmd = 
                    match cmd with
                    | :? 'TCmd as cmd ->
                        sb.GetId cmd
                    | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())
                 member this.CmdType = typeof<'TCmd>
                 member this.Handler state cmd =
                    choose {
                        match cmd with
                        | :? 'TCmd as cmd -> 
                            let! validated = sb.Validate cmd state
                            return sb.Handler validated
                        | _ -> return! Choice2Of2 <| Seq.singleton (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>)
                    }
        }
    static member ToAdded<'TState,'TEvent,'TId> (sb : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TValidatedCmd>) : IHandler<'TState,'TEvent,'TId> = {
            new IHandler<'TState,'TEvent,'TId> with
                member x.add handlers =
                    let cmdInterface = CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TValidatedCmd>.ToInterface sb
                    handlers.AddCommandHandler cmdInterface
        }

module AggregateActionBuilder =
    let simpleHandler<'TId,'TCmd,'TEvent,'TState> (f : 'TCmd -> 'TEvent) =
        {
            GetId = MagicMapper.magicId<'TId>
            Validate = (fun cmd _ -> Choice1Of2 cmd)
            Handler = f >> Seq.singleton
        } : CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TCmd> 

    let buildCmd (handler: CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TCmd>) = CommandHandler<'TCmd, 'TState, 'TId, 'TEvent, 'TCmd>.ToAdded handler

    let getLinkerInterface<'TLinkEvent,'TEvent,'TId> fId : IEventLinker<'TEvent,'TId> = {
        new IEventLinker<'TEvent,'TId> with
            member x.EventType = typeof<'TLinkEvent>
            member x.GetId (event : obj) = 
                match event with
                | :? 'TLinkEvent as event -> fId event
                | _ -> failwith (sprintf "Expecting event of type: %A received %A" typeof<'TLinkEvent> (event.GetType()))
    }
        
    let linkEvent<'TLinkEvent,'TEvent,'TId,'TState> fId (linkEvent : 'TLinkEvent -> 'TEvent) = {
        new IHandler<'TState,'TEvent,'TId> with
            member x.add handlers =
                let linkerInterface = (getLinkerInterface<'TLinkEvent,'TEvent,'TId> fId)
                handlers.AddEventLinker linkerInterface
    }

module Aggregate2 = 
    type AggregateBuilder<'TState,'TEvent,'TId> () = 
        member this.Zero() = AggregateHandlers<'TState,'TEvent,'TId>.Empty

        member x.Delay(f : unit -> AggregateHandlers<'TState,'TEvent,'TId>) = f ()

        member this.Yield(x:IHandler<'TState,'TEvent,'TId>) :  AggregateHandlers<'TState,'TEvent,'TId> =
            let empty = AggregateHandlers<'TState,'TEvent,'TId>.Empty 
            let result = x.add empty
            result
        member this.Combine (a:AggregateHandlers<'TState,'TEvent,'TId>,b:AggregateHandlers<'TState,'TEvent,'TId>) =
            a.Combine b

//        [<CustomOperation("ldc_i4", MaintainsVariableSpace=true)>]
//        member __.Ldc_I4((Instrs f : Instrs<'a,'r,_>, j), [<ProjectionParameter>]h:_->int) : Instrs<V<int> * 'a,'r,Nok> * _ =
//            Instrs(f +> fun s -> s.ilg.Emit(OpCodes.Ldc_I4, h j)), j

    let aggregate<'TState,'TEvent,'TId> = new AggregateBuilder<'TState,'TEvent,'TId>()