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
    with 
        interface IAggregateType 
            with member this.Name with get() = 
                                           match this with
                                           | Teacher -> "Teacher"
                                           | Report -> "Report"
                                           | TeacherReport -> "TeacherReport"

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
        |> NamedStateBuilder.withName "TeacherId"

    let handlers = 
        aggregate<TeacherState,TeacherEvents,TeacherId,AggregateType> 
            AggregateType.Teacher
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

type ChangeReportNameCommand = {
    ReportId : ReportId
    Name : string
}

type ReportNameChangedEvent = {
    ReportId : ReportId
    Name : string
}

type ReportEvents =
    | Added of ReportAddedEvent
    | NameChanged of ReportNameChangedEvent

module Report =
    let handlers =
        aggregate<unit,ReportEvents,ReportId,AggregateType> 
            AggregateType.Report
            {
                let addReport (x : AddReportCommand) =
                   Added { ReportId = x.ReportId
                           TeacherId = x.TeacherId
                           Name = x.Name } 

                yield buildSimpleCmdHandler NamedStateBuilder.nullStateBuilder addReport

                let changeName (x : ChangeReportNameCommand) =
                    NameChanged { ReportId = x.ReportId
                                  Name = x.Name }

                yield buildSimpleCmdHandler NamedStateBuilder.nullStateBuilder changeName

                // create report for each teacher when they are added
                let createTeacherReport (evt : TeacherAddedEvent) =
                    seq {
                        yield Added {
                            ReportId = { Id = evt.TeacherId.Id } 
                            TeacherId = evt.TeacherId; 
                            Name = "Custom teacher report"
                        }
                    }

                yield onEvent (fun (x:TeacherAddedEvent) -> { Id = x.TeacherId.Id }) StateBuilder.NoState createTeacherReport
            }

type TeacherReportEvents =
    | TeacherAdded of TeacherAddedEvent
    | ReportAdded of ReportAddedEvent

module TeacherReport =
    let handlers =
        aggregate<unit,TeacherReportEvents,TeacherId, AggregateType> 
            AggregateType.TeacherReport
            {
                yield linkEvent (fun (x:TeacherAddedEvent) -> x.TeacherId) TeacherReportEvents.TeacherAdded
                yield linkEvent (fun (x:ReportAddedEvent) -> x.TeacherId) TeacherReportEvents.ReportAdded
            }

open Xunit
open FsUnit.Xunit
open Eventful.Testing
open Eventful.Testing.TestHelpers

module TeacherTests = 
    let teacherHandlers =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate Teacher.handlers
        |> EventfulHandlers.addAggregate Report.handlers 
        |> EventfulHandlers.addAggregate TeacherReport.handlers 

    let newTestSystem = TestSystem.Empty teacherHandlers

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given empty When Add Teacher Then TeacherAddedEvent is produced`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let result = 
            newTestSystem
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
    [<Trait("category", "unit")>]
    let ``Given Teacher Exists When Add Teacher Then Error`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let result = 
            newTestSystem
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
    [<Trait("category", "unit")>]
    let ``When AddTeacherCommand has no names Then validation errors are returned`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = ""
            LastName = ""
        }

        let result = newTestSystem.RunCommand command

        result.LastResult |> should containError "FirstName must not be blank"
        result.LastResult |> should containError "LastName must not be blank"

    let buildStreamId (aggregateType : IAggregateType) (id : IIdentity) =
        sprintf "%s-%s" aggregateType.Name id.GetId

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given TeacherAddedEvent When run Then Report is created`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let result = 
            newTestSystem
            |> TestSystem.runCommand command

        let stateBuilder = 
            StateBuilder.Empty None
            |> StateBuilder.addHandler (fun _ (evt : ReportAddedEvent) -> Some evt.Name)

        let stream = buildStreamId AggregateType.Report { Id = teacherId.Id }

        let reportName = result.EvaluateState stream stateBuilder
        reportName |> should equal (Some "Custom teacher report")

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given Existing Teacher When Report added Then teacher report count is incrimented`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        let reportId =  { ReportId.Id = Guid.NewGuid() }
        
        let result = 
            newTestSystem.Run 
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

        let stream = buildStreamId AggregateType.Report reportId
        let state = result.EvaluateState stream stateBuilder

        state |> should equal 1

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Given Report added When Name changed Then State reflects new name`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        let reportId =  { ReportId.Id = Guid.NewGuid() }
        
        let result = 
            newTestSystem.Run 
                [{
                    AddReportCommand.ReportId = reportId
                    TeacherId = teacherId
                    Name = "Test Report" }]

        let stateBuilder = 
            StateBuilder.Empty None
            |> StateBuilder.addHandler (fun s (e:ReportAddedEvent) -> Some e.Name)
            |> StateBuilder.addHandler (fun s (e:ReportNameChangedEvent) -> Some e.Name)

        let stream = buildStreamId AggregateType.Report reportId
        let state = result.EvaluateState stream stateBuilder

        state |> should equal (Some "Test Report")

        let result = 
            result.Run 
                [{
                    ChangeReportNameCommand.ReportId = reportId
                    Name = "New Name" }]

        let state = result.EvaluateState stream stateBuilder

        state |> should equal (Some "New Name")