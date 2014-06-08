namespace SchoolReports

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx.Choice
open Eventful.Aggregate2

type TeacherId = {
    Id : Guid
}

type TeacherAddedEvent = {
    TeacherId : TeacherId
    FirstName : string
    LastName : string
}

type AddTeacherCommand = {
    TeacherId : TeacherId
    FirstName : string
    LastName : string
}

type TeacherState = {
    TeacherId : TeacherId
}

type TeacherEvents =
    | Added of TeacherAddedEvent

open Eventful.AggregateActionBuilder
module Teacher =
    let handlers = 
        aggregate<unit,TeacherEvents,TeacherId> {
            let addTeacher (x : AddTeacherCommand) =
               Added { TeacherId = x.TeacherId
                       FirstName = x.FirstName
                       LastName = x.LastName } 

            yield addTeacher
                  |> simpleHandler
                  |> buildCmd
        }

type TeacherReportEvents =
    | TeacherAdded of TeacherAddedEvent

module TeacherReports =
    let handlers =
        aggregate<unit,TeacherReportEvents,TeacherId> {
            yield linkEvent (fun (x:TeacherAddedEvent) -> x.TeacherId) TeacherReportEvents.TeacherAdded
        }

open Xunit
open FsUnit.Xunit

type Aggregate (commandTypes : Type list, runCommand : obj -> obj list) =
    member x.CommandTypes = commandTypes
    member x.Run (cmd:obj) =
        runCommand cmd 
    
type TestSystem (aggregates : seq<Aggregate>, lastEvents : list<obj>) =
    member x.Aggregates = aggregates

    member x.LastEvents = lastEvents

    member x.Run (cmd : obj) =
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
        new TestSystem(aggregates, result)

module TeacherTests = 
    let toAggregate (handlers : AggregateHandlers<'TState, 'TEvents, 'TId>) =
        let commandTypes = 
            handlers.CommandHandlers
            |> Seq.map (fun x -> x.CmdType)
            |> Seq.toList

        let runCmd (cmd : obj) =
            let cmdType = cmd.GetType()
            let handler = 
                handlers.CommandHandlers
                |> Seq.find (fun x -> x.CmdType = cmdType)
            handler.Handler None cmd
            |> function
            | Choice1Of2 events -> events |> Seq.map (fun x -> x :> obj) |> List.ofSeq
            | _ -> []

        new Aggregate(commandTypes, runCmd)

    [<Fact>]
    let ``Given empty When Add Teacher Then TeacherAddedEvent is produced`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let teacherAggregate = toAggregate Teacher.handlers
        let testSystem = new TestSystem(Seq.singleton teacherAggregate, [])
        let result = testSystem.Run command

        let expectedEvent = Added {
            TeacherAddedEvent.TeacherId = teacherId
            FirstName = command.FirstName
            LastName = command.LastName } :> obj

        result.LastEvents |> should equal [ expectedEvent ]