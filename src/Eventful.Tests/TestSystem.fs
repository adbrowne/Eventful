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
        runCommand : obj -> ('TAggregateType -> string) -> EventStreamProgram<CommandResult>, 
        getId : obj -> IIdentity, aggregateType : 'TAggregateType
    ) =
    member x.CommandTypes = commandTypes
    member x.GetId = getId
    member x.AggregateType = aggregateType
    member x.Run (cmd:obj) =
        runCommand cmd
    
open FSharpx.Collections

type TestEventStore = {
    Events : Map<string,Vector<EventStreamEvent>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let empty : TestEventStore = { Events = Map.empty }
    let addEvent (stream, event, metadata) (store : TestEventStore) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let streamEvents' = streamEvents |> Vector.conj (Event (event, metadata))
        { store with Events = store.Events |> Map.add stream streamEvents' }

open Eventful.EventStream
open FSharpx.Option

module TestInterpreter =
    let rec interpret prog (eventStore : TestEventStore) (values : Map<EventToken,obj>) (writes : Vector<string * int * EventStreamEvent>)= 
        match prog with
        | FreeEventStream (ReadFromStream (stream, eventNumber, f)) -> 
            let readEvent = maybe {
                    let! streamEvents = eventStore.Events |> Map.tryFind stream
                    let! eventStreamData = streamEvents |> Vector.tryNth eventNumber
                    return
                        match eventStreamData with
                        | Event (evt, _) -> 
                            let token = 
                                {
                                    Stream = stream
                                    Number = eventNumber
                                    EventType = evt.GetType().Name
                                }
                            (token, evt)
                        | EventLink _ -> failwith "todo"
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
        | FreeEventStream (WriteToStream (stream, expectedValue, events, next)) ->
            let addEvent w evnetStreamData = 
                w |> Vector.conj (stream, expectedValue, evnetStreamData) 
            let writes' = Seq.fold addEvent writes events
            interpret (next WriteSuccess) eventStore values writes'
        | FreeEventStream (NotYetDone g) ->
            let next = g ()
            interpret next eventStore values writes
        | Pure result ->
            let writeEvent store (stream, exepectedValue, eventStreamData) =
                // todo check expected value
                let streamEvents = 
                    store.Events 
                    |> Map.tryFind stream 
                    |> FSharpx.Option.getOrElse Vector.empty
                    |> Vector.conj eventStreamData
                
                { store with Events = store.Events |> Map.add stream streamEvents }

            let eventStore' = 
                writes |> Vector.fold writeEvent eventStore
            (eventStore',result)
    
type EventfulHandler<'T> = EventfulHandler of Type * (obj ->  EventStreamProgram<'T>)

type MyEventResult = unit

type EventfulHandlers
    (
        commandHandlers : Map<string, EventfulHandler<CommandResult>>, 
        eventHandlers : Map<string, EventfulHandler<MyEventResult>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AddCommandHandler = function
        | EventfulHandler(cmdType,_) as handler -> 
            let cmdTypeFullName = cmdType.FullName
            let commandHandlers' = commandHandlers |> Map.add cmdTypeFullName handler
            new EventfulHandlers(commandHandlers', eventHandlers)
    member x.AddEventHandler (eventType:Type) handler =
        let evtName = eventType.Name
        let eventHandlers' = eventHandlers |> Map.add evtName handler
        new EventfulHandlers(commandHandlers, eventHandlers')

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventfulHandlers = 
    let empty = new EventfulHandlers(Map.empty, Map.empty)
    

type TestSystem
    (
        handlers : EventfulHandlers, 
        lastResult : CommandResult, 
        allEvents : TestEventStore 
    ) =

    member x.RunCommand (cmd : obj) =    
        let cmdType = cmd.GetType()
        let cmdTypeFullName = cmd.GetType().FullName
        let sourceMessageId = Guid.NewGuid()
        let handler = 
            handlers.CommandHandlers
            |> Map.tryFind cmdTypeFullName
            |> function
            | Some (EventfulHandler(_, handler)) -> handler
            | None -> failwith <| sprintf "Could not find handler for %A" cmdType

        let program = handler cmd
        let (allEvents', result) = TestInterpreter.interpret program allEvents Map.empty Vector.empty

        new TestSystem(handlers, result, allEvents')

    member x.Handlers = handlers

    member x.LastResult = lastResult

    member x.AddAggregate (aggregateHandlers : AggregateHandlers<'TState, 'TEvents, 'TId, 'TAggregateType>) =
        
        let aggregateTypeString = aggregateHandlers.AggregateType.ToString()
        let commandHandlers = 
            aggregateHandlers.CommandHandlers
            |> Seq.map (fun x -> 
                 EventfulHandler(x.CmdType, x.Handler aggregateTypeString)
               )
        
        let handlers' =
            commandHandlers
            |> Seq.fold (fun (s:EventfulHandlers) h -> s.AddCommandHandler h) handlers

        new TestSystem(handlers', lastResult, allEvents)

    member x.Run (cmds : obj list) =
        cmds
        |> List.fold (fun (s:TestSystem) cmd -> s.RunCommand cmd) x

    member x.EvaluateState<'TState> (stream : string) (stateBuilder : StateBuilder<'TState>) =
        let streamEvents = 
            allEvents.Events 
            |> Map.tryFind stream
            |> function
            | Some events -> 
                events
            | None -> Vector.empty

        streamEvents
        |> Vector.map (function
            | Event (obj, _) ->
                obj
            | EventLink (streamId, eventNumber, _) ->
                allEvents.Events
                |> Map.find streamId
                |> Vector.nth eventNumber
                |> (function
                        | Event (obj, _) -> obj
                        | _ -> failwith ("found link to a link")))
        |> Vector.fold stateBuilder.Run stateBuilder.InitialState

    static member Empty =
        new TestSystem(EventfulHandlers.empty, Choice1Of2 List.empty, TestEventStore.empty)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestSystem = 
    let runCommand x (y:TestSystem) = y.RunCommand x