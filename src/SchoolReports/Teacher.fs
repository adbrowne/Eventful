namespace SchoolReports

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx
open FSharpx.Choice
open Eventful.Aggregate

type AggregateType =
| Teacher
| Report
| TeacherReport

type TeacherId = 
    {
        Id : Guid
    } 
    interface IIdentity with
        member this.GetId = MagicMapper.getGuidId this

type ReportId = 
    {
        Id : Guid
    }
    interface IIdentity with
        member this.GetId = MagicMapper.getGuidId this

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
open Eventful.Validation

module Teacher =
    let stateBuilder = 
        StateBuilder.Empty { TeacherState.TeacherId = { TeacherId.Id = Guid.NewGuid() }}
        |> StateBuilder.addHandler (fun s (e:TeacherAddedEvent) -> { s with TeacherId = e.TeacherId })

    let handlers = 
        aggregate<TeacherState,TeacherEvents,TeacherId,AggregateType> 
            AggregateType.Teacher stateBuilder
            {
               let addTeacher (cmd : AddTeacherCommand) =
                   Added { 
                       TeacherId = cmd.TeacherId
                       FirstName = cmd.FirstName
                       LastName = cmd.LastName 
               } 

               yield addTeacher
                     |> simpleHandler stateBuilder
                     |> ensureFirstCommand
                     |> addValidator (CommandValidator (notBlank (fun x -> x.FirstName) "FirstName"))
                     |> addValidator (CommandValidator (notBlank (fun x -> x.LastName) "LastName"))
                     |> buildCmd
            }

type AddReportCommand = {
    ReportId : ReportId
    TeacherId : TeacherId
    Name : string
}

type ReportAddedEvent = {
    ReportId : ReportId
    TeacherId : TeacherId
    Name : string
}

type ReportEvents =
    | Added of ReportAddedEvent

module Report =
    let handlers =
        aggregate<unit,ReportEvents,ReportId,AggregateType> 
            AggregateType.Report (StateBuilder.Empty ())
            {
                let addReport (x : AddReportCommand) =
                   Added { ReportId = x.ReportId
                           TeacherId = x.TeacherId
                           Name = x.Name } 

                yield buildSimpleCmdHandler (StateBuilder.Empty ()) addReport
            }

type TeacherReportEvents =
    | TeacherAdded of TeacherAddedEvent
    | ReportAdded of ReportAddedEvent

module TeacherReport =
    let handlers =
        aggregate<unit,TeacherReportEvents,TeacherId, AggregateType> 
            AggregateType.TeacherReport (StateBuilder.Empty ())
            {
                yield linkEvent (fun (x:TeacherAddedEvent) -> x.TeacherId) TeacherReportEvents.TeacherAdded
                yield linkEvent (fun (x:ReportAddedEvent) -> x.TeacherId) TeacherReportEvents.ReportAdded
            }

open Xunit
open FsUnit.Xunit
open Eventful.Testing
open Eventful.Testing.TestHelpers

module TeacherTests = 
    let newTestSystem () =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate Teacher.handlers
        |> EventfulHandlers.addAggregate Report.handlers 
        |> EventfulHandlers.addAggregate TeacherReport.handlers 
        |> TestSystem.Empty

    [<Fact>]
    let ``Given empty When Add Teacher Then TeacherAddedEvent is produced`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let result = 
            newTestSystem()
            |> TestSystem.runCommand command

        let expectedEvent : TeacherAddedEvent -> bool = function
            | {
                    TeacherId = Equals teacherId
                    FirstName = Equals command.FirstName
                    LastName = Equals command.LastName
              } -> true
            | _ -> false

        result.LastResult
        |> should beSuccessWithEvent expectedEvent

    [<Fact>]
    let ``Given Teacher Exists When Add Teacher Then Error`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let result = 
            newTestSystem()
            |> TestSystem.runCommand command
            |> TestSystem.runCommand command // run a second time - oops
             
        let expectedEvent : TeacherAddedEvent -> bool = function
            | {
                    TeacherId = Equals teacherId
                    FirstName = Equals command.FirstName
                    LastName = Equals command.LastName
              } -> true
            | _ -> false

        result.LastResult |> should containError "Must be the first command"

    [<Fact>]
    let ``When AddTeacherCommand has no names Then validation errors are returned`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = ""
            LastName = ""
        }

        let result = newTestSystem().RunCommand command

        result.LastResult |> should containError "FirstName must not be blank"
        result.LastResult |> should containError "LastName must not be blank"

    [<Fact>]
    let ``Given Existing Teacher When Report added Then teacher report count is incrimented`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        let reportId =  { ReportId.Id = Guid.NewGuid() }
        
        let result = 
            newTestSystem().Run 
                [{
                    AddTeacherCommand.TeacherId = teacherId
                    FirstName = "Andrew"
                    LastName = "Browne" }
                 {
                    AddReportCommand.ReportId = reportId
                    TeacherId = teacherId
                    Name = "Test Report" }]

        let stateBuilder = 
            StateBuilder.Empty 0
            |> StateBuilder.addHandler (fun s (e:ReportAddedEvent) -> s + 1)

        let stream = sprintf "%s-%s" (AggregateType.Report.ToString()) ((reportId :> IIdentity).GetId)
        let state = result.EvaluateState stream stateBuilder

        state |> should equal 1