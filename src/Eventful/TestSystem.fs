namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections

type Aggregate (commandTypes : Type list, runCommand : obj -> obj list) =
    member x.CommandTypes = commandTypes
    member x.Run (cmd:obj) =
        runCommand cmd 
    
open FSharpx.Collections

type TestSystem (aggregates : list<Aggregate>, lastEvents : list<obj>, allEvents : Vector<obj>) =
    member x.RunCommand (cmd : obj) =    
        let cmdType = cmd.GetType()
        let aggregate = 
            aggregates
            |> Seq.filter (fun a -> a.CommandTypes |> Seq.exists (fun c -> c = cmdType))
            |> Seq.toList
            |> function
            | [aggregate] -> aggregate
            | [] -> failwith <| sprintf "Could not find aggregate to handle %A" cmdType
            | xs -> failwith <| sprintf "Found more than one aggreate %A to handle %A" xs cmdType

        let result = aggregate.Run cmd 
        let allEvents' = result |> Vector.ofSeq |> Vector.append allEvents
        new TestSystem(aggregates, result, allEvents')

    member x.Aggregates = aggregates

    member x.LastEvents = lastEvents

    member x.AddAggregate (handlers : AggregateHandlers<'TState, 'TEvents, 'TId>) =
        let commandTypes = 
            handlers.CommandHandlers
            |> Seq.map (fun x -> x.CmdType)
            |> Seq.toList

        let unwrapper = MagicMapper.getUnwrapper<'TEvents>()

        let runCmd (cmd : obj) =
            let cmdType = cmd.GetType()
            let handler = 
                handlers.CommandHandlers
                |> Seq.find (fun x -> x.CmdType = cmdType)
            handler.Handler None cmd
            |> function
            | Choice1Of2 events -> events |> Seq.map unwrapper |> List.ofSeq
            | _ -> []

        let aggregate = new Aggregate(commandTypes, runCmd)
        new TestSystem(aggregate::aggregates, lastEvents, allEvents)

    member x.Run (cmds : obj list) =
        cmds
        |> List.fold (fun (s:TestSystem) cmd -> s.RunCommand cmd) x

    member x.EvaluateState<'TState>(stateBuilder : StateBuilder<'TState>) =
        allEvents
        |> Vector.toSeq
        |> Seq.fold stateBuilder.Run stateBuilder.InitialState
    static member Empty =
        new TestSystem(List.empty, List.empty, Vector.empty)