namespace SchoolReports

open System
open Microsoft.FSharp.Core
open Eventful
open Eventful.Aggregate

type SchoolReportMetadata = {
    AggregateId : Guid
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

    let emptyMetadata = { SourceMessageId = String.Empty; MessageId = Guid.Empty; AggregateId = Guid.Empty }

    let inline buildMetadata aggregateId messageId sourceMessageId = { 
            SourceMessageId = sourceMessageId 
            MessageId = messageId 
            AggregateId = aggregateId }

    let inline withMetadata f cmd = 
        let metadata aggregateId messageId sourceMessageId = { 
            SourceMessageId = sourceMessageId 
            MessageId = messageId 
            AggregateId = aggregateId }

        let cmdResult = f cmd
        (cmdResult, metadata)

    let inline simpleHandler s f = 
        Eventful.AggregateActionBuilder.simpleHandler systemConfiguration s (withMetadata f)
    let inline buildSimpleCmdHandler s f = 
        Eventful.AggregateActionBuilder.buildSimpleCmdHandler systemConfiguration s (withMetadata f)
    let inline onEvent fId s f = 
        let withMetadata f = f >> Seq.map (fun x -> (x, buildMetadata))
        Eventful.AggregateActionBuilder.onEvent systemConfiguration fId s (withMetadata f)
    let inline linkEvent fId f = 
        let withMetadata f = f >> (fun x -> (x, { SourceMessageId = String.Empty; MessageId = Guid.Empty; AggregateId = Guid.Empty }))
        Eventful.AggregateActionBuilder.linkEvent systemConfiguration fId f emptyMetadata

open SchoolReportHelpers

module Teacher =
    let eventCountStateBuilder =
        StateBuilder.Empty "EventCount" 0
        |> StateBuilder.allEventsHandler (fun (m : SchoolReportMetadata) -> { TeacherId.Id = m.AggregateId }) (fun (s, _, _) -> s + 1)

    let isFirstCommand i =
        match i with
        | 0 -> Seq.empty
        | _ -> [(None, "Must be the first command")] |> List.toSeq

    let ensureFirstCommand x = addValidator (StateValidator (fun r -> r.Register eventCountStateBuilder isFirstCommand)) x 

    let stateBuilder = 
        StateBuilder.Empty "TeacherId" { TeacherState.TeacherId = { TeacherId.Id = Guid.NewGuid() }}
        |> StateBuilder.handler (fun (e:TeacherAddedEvent) _ -> e.TeacherId) (fun (s, e, _) -> { s with TeacherId = e.TeacherId })

    let getStreamName () (id:TeacherId) =
        sprintf "Teacher-%s" (id.Id.ToString("N"))

    let getAggregateId (id : TeacherId) = id.Id

    let teacherIdToAggregateId (x : TeacherId) = x.Id

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
        toAggregateDefinition getStreamName getStreamName teacherIdToAggregateId cmdHandlers Seq.empty

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
    let inline getReportId (a: ^a) = 
        (^a : (member ReportId: ReportId) (a))

    let getStreamName () (id:ReportId) =
        sprintf "Report-%s" (id.Id.ToString("N"))

    let reportIdToAggregateId (x : ReportId) = x.Id

    let cmdHandlers =
        seq {
            let addReport (x : AddReportCommand) =
               Added { ReportId = x.ReportId
                       TeacherId = x.TeacherId
                       Name = x.Name } 

            yield buildSimpleCmdHandler StateBuilder.nullStateBuilder addReport

            let changeName (x : ChangeReportNameCommand) =
                NameChanged { ReportId = x.ReportId
                              Name = x.Name }

            yield buildSimpleCmdHandler StateBuilder.nullStateBuilder changeName
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

            yield onEvent (fun (x:TeacherAddedEvent) -> { Id = x.TeacherId.Id }) StateBuilder.nullStateBuilder createTeacherReport
        }

    let handlers =
        toAggregateDefinition getStreamName getStreamName reportIdToAggregateId cmdHandlers evtHandlers

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
        toAggregateDefinition getStreamName getStreamName Teacher.teacherIdToAggregateId Seq.empty evtHandlers

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

        result.LastResult |> should containError (CommandError "Must be the first command")

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

        result.LastResult |> should containError (CommandFieldError ("FirstName", "FirstName must not be blank"))
        result.LastResult |> should containError (CommandFieldError ("LastName", "LastName must not be blank"))

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
            StateBuilder.Empty "reportName" None
            |> StateBuilder.handler (fun (evt : ReportAddedEvent) (m : SchoolReportMetadata) -> evt.TeacherId) (fun (s, (evt : ReportAddedEvent), m) -> Some evt.Name)

        let stream = Report.getStreamName () { Id = teacherId.Id }

        let reportName = result.EvaluateState stream teacherId stateBuilder
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
            StateBuilder.Empty "reportCount" 0
            |> StateBuilder.handler (fun (e:ReportAddedEvent) m -> e.ReportId) (fun (s, (e:ReportAddedEvent), m) -> s + 1)

        let stream = Report.getStreamName () reportId
        let state = result.EvaluateState stream reportId stateBuilder

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
            StateBuilder.Empty "reportName" None
            |> StateBuilder.handler (fun (e:ReportAddedEvent) m -> e.ReportId) (fun (s, (e:ReportAddedEvent), m) -> Some e.Name)
            |> StateBuilder.handler (fun (e:ReportNameChangedEvent) m -> e.ReportId) (fun (s, (e:ReportNameChangedEvent), m) -> Some e.Name)

        let stream = Report.getStreamName () reportId
        let state = result.EvaluateState stream reportId stateBuilder

        state |> should equal (Some "Test Report")

        let result = 
            result.Run 
                [{
                    ChangeReportNameCommand.ReportId = reportId
                    Name = "New Name" }]

        let state = result.EvaluateState stream reportId stateBuilder

        state |> should equal (Some "New Name")