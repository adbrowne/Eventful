namespace EmergencyRoom

open System
open Xunit
open FsCheck.Xunit
open FsCheck
open FsUnit.Xunit
open FSharpx
open FSharpx.Collections
open Eventful
open Eventful.Testing
open Eventful.Testing.TestHelpers

module VisitTests = 

    let emptyTestSystem =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate Visit.handlers
        |> TestSystem.Empty

    let runCommandsAndApplyEventsToViewModel (cmds : seq<obj>) (visitId : VisitId) =
        let streamName = Visit.getStreamName () visitId
        let applyCommand = flip TestSystem.runCommand
        cmds |> Seq.fold applyCommand emptyTestSystem

    [<Fact>]
    let ``Given empty visit When Register Patient Then Patient Registered Event emitted`` () : unit =
        let visitId = VisitId.New()
        let patientId = PatientId.New()
        let registrationTime = DateTime.UtcNow
        let command : RegisterPatientCommand = {
            VisitId = visitId
            PatientId = patientId
            RegistrationTime = registrationTime
        }

        let result = 
            emptyTestSystem
            |> TestSystem.runCommand command

        let expectedEvent : PatientRegisteredEvent -> bool = function
            | {
                    VisitId = Equals visitId
                    PatientId = Equals patientId
                    RegistrationTime = Equals registrationTime
              } -> true
            | _ -> false
        
        result.LastResult |> should beSuccessWithEvent expectedEvent

    [<Property>]
    let ``All valid command sequences successfully apply to the Read Model`` 
        (visitId : VisitId) =
        Prop.forAll 
            (TestHelpers.getCommandsForAggregate Visit.handlers ("VisitId",visitId)) 
            (fun cmds -> runCommandsAndApplyEventsToViewModel cmds visitId)