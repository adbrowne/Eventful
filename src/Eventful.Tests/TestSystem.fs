namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx.Choice

type Aggregate<'TAggregateType>(commandTypes : Type list, runCommand : obj -> Choice<obj list, ValidationFailure seq>, getId : obj -> IIdentity, aggregateType : 'TAggregateType) =
    member x.CommandTypes = commandTypes
    member x.GetId = getId
    member x.AggregateType = aggregateType
    member x.Run (cmd:obj) =
        runCommand cmd 
    
open FSharpx.Collections

type TestEventStore = {
    Events : Map<string,Vector<(obj * EventMetadata)>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let empty : TestEventStore = { Events = Map.empty }
    let addEvent (stream, event, metadata) (store : TestEventStore) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let streamEvents' = streamEvents |> Vector.conj (event, metadata)
        { store with Events = store.Events |> Map.add stream streamEvents' }

type Settings<'TAggregateType,'TCommandMetadata> = {
    GetStreamName : 'TCommandMetadata -> 'TAggregateType -> IIdentity -> string
}

type TestSystem<'TAggregateType> 
    (
        aggregates : list<Aggregate<'TAggregateType>>, 
        lastResult : Choice<list<string * obj * EventMetadata>,ValidationFailure seq>, 
        allEvents : TestEventStore, 
        settings : Settings<'TAggregateType,unit>
    ) =
    let testTenancy = Guid.Parse("A1028FC2-67D2-4F52-A69F-714A0F571D8A") :> obj
    member x.RunCommand (cmd : obj) =    
        let cmdType = cmd.GetType()
        let sourceMessageId = Guid.NewGuid()
        let aggregate = 
            aggregates
            |> Seq.filter (fun a -> a.CommandTypes |> Seq.exists (fun c -> c = cmdType))
            |> Seq.toList
            |> function
            | [aggregate] -> aggregate
            | [] -> failwith <| sprintf "Could not find aggregate to handle %A" cmdType
            | xs -> failwith <| sprintf "Found more than one aggreate %A to handle %A" xs cmdType

        let id = aggregate.GetId cmd
        let streamName = settings.GetStreamName () aggregate.AggregateType id

        let result = 
            choose {
                let! result = aggregate.Run cmd
                return
                    result
                    |> Seq.map (fun evt -> 
                                let metadata = {
                                    SourceMessageId = sourceMessageId
                                    MessageId = Guid.NewGuid() 
                                }
                                (streamName,evt, metadata))
                    |> List.ofSeq
            }
            
        let allEvents' =
            match result with
            | Choice1Of2 resultingEvents ->
                resultingEvents
                |> Seq.fold (fun s e -> s |> TestEventStore.addEvent e) allEvents
            | _ -> allEvents

        new TestSystem<'TAggregateType>(aggregates, result, allEvents',settings)

    member x.Aggregates = aggregates

    member x.LastResult = lastResult

    member x.AddAggregate (handlers : AggregateHandlers<'TState, 'TEvents, 'TId, 'TAggregateType>) =
        let commandTypes = 
            handlers.CommandHandlers
            |> Seq.map (fun x -> x.CmdType)
            |> Seq.toList

        let unwrapper = MagicMapper.getUnwrapper<'TEvents>()

        let getHandler cmdType =    
            handlers.CommandHandlers
            |> Seq.find (fun x -> x.CmdType = cmdType)

        let getId (cmd : obj) =
            let handler = getHandler (cmd.GetType())
            handler.GetId cmd :> IIdentity
            
        let runCmd (cmd : obj) =
            let handler = getHandler (cmd.GetType())
            choose {
                let! result = handler.Handler None cmd
                return
                    result 
                    |> Seq.map unwrapper
                    |> List.ofSeq
            }

        let aggregate = new Aggregate<_>(commandTypes, runCmd, getId, handlers.AggregateType)
        new TestSystem<'TAggregateType>(aggregate::aggregates, lastResult, allEvents, settings)

    member x.Run (cmds : obj list) =
        cmds
        |> List.fold (fun (s:TestSystem<_>) cmd -> s.RunCommand cmd) x

    member x.EvaluateState<'TState> (stream : string) (stateBuilder : StateBuilder<'TState>) =
        let streamEvents = 
            allEvents.Events 
            |> Map.tryFind stream
            |> function
            | Some events -> 
                events
            | None -> Vector.empty

        streamEvents
        |> Vector.map fst
        |> Vector.fold stateBuilder.Run stateBuilder.InitialState

    static member Empty settings =
        new TestSystem<_>(List.empty, Choice1Of2 List.empty, TestEventStore.empty, settings)