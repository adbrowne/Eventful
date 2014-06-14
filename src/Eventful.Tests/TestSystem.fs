namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx.Choice

type Aggregate<'TAggregateType>
    (
        commandTypes : Type list, 
        runCommand : obj -> Option<seq<obj>> -> Choice<obj list, NonEmptyList<ValidationFailure>>, 
        getId : obj -> IIdentity, aggregateType : 'TAggregateType
    ) =
    member x.CommandTypes = commandTypes
    member x.GetId = getId
    member x.AggregateType = aggregateType
    member x.Run (cmd:obj) (events:Option<seq<obj>>) =
        runCommand cmd events 
    
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

open Eventful.EventStream

module TestInterpreter =
    let rec interpret prog (eventStore : TestEventStore) (values : Map<EventToken,obj>) = 
        match prog with
        | FreeEventStream (ReadFromStream (stream, eventNumber, g)) -> 
            let streamEvents = eventStore.Events.[stream] // todo what if it doesn't exist
            let (evt, metadata) = streamEvents.[eventNumber] // todo what if it doesn't exist
            let eventToken = {
                                Stream = stream
                                Number = eventNumber
                                EventType = evt.GetType().Name
                             }

            let next = g eventToken            
            let values' = values |> Map.add eventToken evt
            interpret next eventStore values'
        | FreeEventStream (ReadValue (token, eventType, g)) ->
            let eventObj = values.[token]
            let next = g eventObj
            interpret next eventStore values
        | FreeEventStream (WriteToStream (_,_,next)) ->
            // todo
            interpret next eventStore values
        | Pure result ->
            result
    
type TestSystem<'TAggregateType> 
    (
        aggregates : list<Aggregate<'TAggregateType>>, 
        lastResult : CommandResult, 
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

        let streamEvents = 
            match allEvents.Events |> Map.tryFind streamName with
            | Some events -> 
                events 
                |> Vector.map fst 
                |> Vector.toSeq
                |> Some
            | None -> None

        let result = 
            choose {
                let! result = aggregate.Run cmd streamEvents
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
            
        let runCmd (cmd : obj) (events : Option<seq<obj>>) =
            let state = 
                match events with
                | Some events ->
                    events
                    |> Seq.fold handlers.StateBuilder.Run handlers.StateBuilder.InitialState
                    |> Some
                | None -> None
            let handler = getHandler (cmd.GetType())
            choose {
                let! result = handler.Handler state cmd
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

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x (y:TestSystem<_>) = y.RunCommand x