namespace SchoolReports

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx.Choice
open Eventful.Aggregate2

type AggregateType =
| Teacher
| Report
| TeacherReport

type TeacherId = {
    Id : Guid
}

type ReportId = {
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
        aggregate<unit,ReportEvents,ReportId> {
            let addReport (x : AddReportCommand) =
               Added { ReportId = x.ReportId
                       TeacherId = x.TeacherId
                       Name = x.Name } 

            yield addReport
                  |> simpleHandler
                  |> buildCmd
        }

type TeacherReportEvents =
    | TeacherAdded of TeacherAddedEvent
    | ReportAdded of ReportAddedEvent

module TeacherReport =
    let handlers =
        aggregate<unit,TeacherReportEvents,TeacherId> {
            yield linkEvent (fun (x:TeacherAddedEvent) -> x.TeacherId) TeacherReportEvents.TeacherAdded
            yield linkEvent (fun (x:ReportAddedEvent) -> x.TeacherId) TeacherReportEvents.ReportAdded
        }

open Xunit
open FsUnit.Xunit
open Eventful.Testing

module TeacherTests = 
    let settings = { 
        GetStreamName = fun tenancy aggregate id -> sprintf "%A-%A-%A" "test" aggregate id 
    }

    let newTestSystem () =
        TestSystem.Empty settings
        |> (fun x -> x.AddAggregate Teacher.handlers AggregateType.Teacher)
        |> (fun x -> x.AddAggregate Report.handlers AggregateType.Report)
        |> (fun x -> x.AddAggregate TeacherReport.handlers AggregateType.TeacherReport)

    [<Fact>]
    let ``Given empty When Add Teacher Then TeacherAddedEvent is produced`` () : unit =
        let teacherId =  { TeacherId.Id = Guid.NewGuid() }
        
        let command : AddTeacherCommand = {
            TeacherId = teacherId
            FirstName = "Andrew"
            LastName = "Browne"
        }

        let result = newTestSystem().RunCommand command

        let expectedEvent = {
            TeacherAddedEvent.TeacherId = teacherId
            FirstName = command.FirstName
            LastName = command.LastName } :> obj

        result.LastEvents 
        |> Seq.map (fun (streamName, evt, _) -> evt)  
        |> Seq.toList
        |> should equal [ expectedEvent ]

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

        let stream = settings.GetStreamName ("test" :> obj) (AggregateType.Report :> obj) (reportId :> obj)
        let state = result.EvaluateState stream stateBuilder

        state |> should equal 1