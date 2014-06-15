namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Option

open Eventful.EventStream

type Aggregate<'TAggregateType>
    (
        commandTypes : Type list, 
        runCommand : obj -> string -> EventStreamProgram<CommandResult>, 
        getId : obj -> IIdentity, aggregateType : 'TAggregateType
    ) =
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

open Eventful.EventStream
open FSharpx.Option

module TestInterpreter =
    let rec interpret prog (eventStore : TestEventStore) (values : Map<EventToken,obj>) (writes : Vector<string * int * obj * EventMetadata>)= 
        match prog with
        | FreeEventStream (ReadFromStream (stream, eventNumber, f)) -> 
            let readEvent = maybe {
                    let! streamEvents = eventStore.Events |> Map.tryFind stream
                    let! (evt, metadata) = streamEvents |> Vector.tryNth eventNumber
                    let eventToken = {
                        Stream = stream
                        Number = eventNumber
                        EventType = evt.GetType().Name
                    }
                    return (eventToken, evt)
                }

            match readEvent with
            | Some (eventToken, evt) -> 
                let next = f (Some eventToken)
                let values' = values |> Map.add eventToken evt
                interpret next eventStore values' writes
            | None ->
                let next = f None
                interpret next eventStore values writes
        | FreeEventStream (ReadValue (token, eventType, g)) ->
            let eventObj = values.[token]
            let next = g eventObj
            interpret next eventStore values writes
        | FreeEventStream (WriteToStream (stream, expectedValue, data, metadata, next)) ->
            let writes' = writes |> Vector.conj (stream, expectedValue, data, metadata)
            interpret next eventStore values writes'
        | FreeEventStream (NotYetDone g) ->
            let next = g ()
            interpret next eventStore values writes
        | Pure result ->
            let writeEvent store (stream, exepectedValue, data, metadata) =
                // todo check expected value
                let streamEvents = 
                    store.Events 
                    |> Map.tryFind stream 
                    |> FSharpx.Option.getOrElse Vector.empty
                    |> Vector.conj (data, metadata)
                
                { store with Events = store.Events |> Map.add stream streamEvents }

            let eventStore' = 
                writes |> Vector.fold writeEvent eventStore
            (eventStore',result)
    
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

        let program = aggregate.Run cmd streamName
        let (allEvents', result) = TestInterpreter.interpret program allEvents Map.empty Vector.empty

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
            
        let runCmd (cmd : obj) stream =
            let handler = getHandler (cmd.GetType())
            handler.Handler cmd stream

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