namespace SchoolReports

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx
open FSharpx.Choice
open Eventful.Aggregate

type SchoolReportMetadata = {
    MessageId: Guid
    SourceMessageId: string
}

type AggregateType =
| Teacher
| Report
| TeacherReport

type TeacherId = 
    {
        Id : Guid
    } 

type ReportId = 
    {
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
open Eventful.Validation

module SchoolReportHelpers =
    let systemConfiguration = { 
        SetSourceMessageId = (fun id metadata -> { metadata with SourceMessageId = id })
        SetMessageId = (fun id metadata -> { metadata with MessageId = id })
    }

    let emptyMetadata = { SourceMessageId = String.Empty; MessageId = Guid.Empty }

    let nullStateBuilder = NamedStateBuilder.nullStateBuilder<SchoolReportMetadata>
    let inline simpleHandler s f = 
        let withMetadata f = f >> (fun x -> (x, emptyMetadata))
        Eventful.AggregateActionBuilder.simpleHandler systemConfiguration s (withMetadata f)
    let inline buildSimpleCmdHandler s f = 
        let withMetadata f = f >> (fun x -> (x, emptyMetadata))
        Eventful.AggregateActionBuilder.buildSimpleCmdHandler systemConfiguration s (withMetadata f)
    let inline onEvent fId s f = 
        let withMetadata f = f >> Seq.map (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty }))
        Eventful.AggregateActionBuilder.onEvent systemConfiguration fId s (withMetadata f)
    let inline linkEvent fId f = 
        let withMetadata f = f >> (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty }))
        Eventful.AggregateActionBuilder.linkEvent systemConfiguration fId f emptyMetadata

open SchoolReportHelpers

module Teacher =
    let stateBuilder = 
        StateBuilder.Empty { TeacherState.TeacherId = { TeacherId.Id = Guid.NewGuid() }}
        |> StateBuilder.addHandler (fun s (e:TeacherAddedEvent) -> { s with TeacherId = e.TeacherId })
        |> NamedStateBuilder.withName "TeacherId"

    let getStreamName () (id:TeacherId) =
        sprintf "Teacher-%s" (id.Id.ToString("N"))

    let cmdHandlers = 
        seq {
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
    let handlers =
        toAggregateDefinition getStreamName getStreamName cmdHandlers Seq.empty

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
    let getStreamName () (id:ReportId) =
        sprintf "Report-%s" (id.Id.ToString("N"))

    let cmdHandlers =
        seq {
            let addReport (x : AddReportCommand) =
               Added { ReportId = x.ReportId
                       TeacherId = x.TeacherId
                       Name = x.Name } 

            yield buildSimpleCmdHandler nullStateBuilder addReport

            let changeName (x : ChangeReportNameCommand) =
                NameChanged { ReportId = x.ReportId
                              Name = x.Name }

            yield buildSimpleCmdHandler nullStateBuilder changeName
        }

    let evtHandlers =
        seq {
            // create report for each teacher when they are added
            let createTeacherReport (evt : TeacherAddedEvent) =
                seq {
                    yield Added {
                        ReportId = { Id = evt.TeacherId.Id } 
                        TeacherId = evt.TeacherId; 
                        Name = "Custom teacher report"
                    }
                }

            yield onEvent (fun (x:TeacherAddedEvent) -> { Id = x.TeacherId.Id }) nullStateBuilder createTeacherReport
        }

    let handlers =
        toAggregateDefinition getStreamName getStreamName cmdHandlers evtHandlers

type TeacherReportEvents =
    | TeacherAdded of TeacherAddedEvent
    | ReportAdded of ReportAddedEvent

module TeacherReport =
    let getStreamName () (id:TeacherId) =
        sprintf "TeacherReport-%s" (id.Id.ToString("N"))

    let evtHandlers =
        seq {
            yield linkEvent (fun (x:TeacherAddedEvent) -> x.TeacherId) TeacherReportEvents.TeacherAdded
            yield linkEvent (fun (x:ReportAddedEvent) -> x.TeacherId) TeacherReportEvents.ReportAdded
        }
    let handlers =
        toAggregateDefinition getStreamName getStreamName Seq.empty evtHandlers

open Xunit
open FsUnit.Xunit
open Eventful.Testing
open Eventful.Testing.TestHelpers

module SchoolReportTestHelpers = 
    let containError = Eventful.Testing.TestHelpers.containError<SchoolReportMetadata>
//    let beSuccessWithEvent<'A> = Eventful.Testing.TestHelpers.beSuccessWithEvent<'A,SchoolReportMetadata>

open SchoolReportTestHelpers

module TeacherTests = 
    let teacherHandlers =
        EventfulHandlers.empty<unit,unit,SchoolReportMetadata>
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
        |> function
        | Choice1Of2 [(stream, evt , metadata)] -> 
            match evt with
            | :? TeacherAddedEvent as evt -> 
                expectedEvent evt
            | _ -> false
        | _ -> false
        |> should be True

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

        result.LastResult |> should containError (None, "Must be the first command")

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

        result.LastResult |> should containError (Some "FirstName", "FirstName must not be blank")
        result.LastResult |> should containError (Some "LastName", "LastName must not be blank")

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

        let stream = Report.getStreamName () { Id = teacherId.Id }

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

        let stream = Report.getStreamName () reportId
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

        let stream = Report.getStreamName () reportId
        let state = result.EvaluateState stream stateBuilder

        state |> should equal (Some "Test Report")

        let result = 
            result.Run 
                [{
                    ChangeReportNameCommand.ReportId = reportId
                    Name = "New Name" }]

        let state = result.EvaluateState stream stateBuilder

        state |> should equal (Some "New Name")