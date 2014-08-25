namespace EmergencyRoom

open System
open Xunit
open FsCheck.Xunit
open FsCheck
open FsUnit.Xunit
open FSharpx
open FSharpx.Collections
open Eventful
open Eventful.Validation
open Eventful.Testing
open Eventful.Testing.TestHelpers

open EmergencyRoom.Visit

module VisitTests = 

    let emptyTestSystem =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate Visit.handlers
        |> TestSystem.Empty

    let runCommandsAndApplyEventsToViewModel (cmds : seq<obj>) (visitId : VisitId) =
        let streamName = Visit.getStreamName () visitId
        let applyCommand = flip TestSystem.runCommand
        let finalTestSystem = cmds |> Seq.fold applyCommand emptyTestSystem
        finalTestSystem.EvaluateState streamName Visit.visitDocumentBuilder |> ignore

    [<Fact>]
    let ``Given empty visit When Register Patient Then Patient Registered Event emitted - equality`` () : unit =
        let visitId = VisitId.New()
        let patientId = PatientId.New()
        let registrationTime = DateTime.UtcNow

        let command : RegisterPatientCommand = {
            VisitId = visitId
            PatientId = patientId
            RegistrationTime = registrationTime
            StreetNumber = "123"
            StreetLine1 = "Test St"
            StreetLine2 = ""
            Suburb = "Testville"
            State = "Victoria"
            Postcode = "3000"
            MedicareNumber = "1234567890"
        }

        let result = 
            Visit.registerPatient (Some false) () command

        let expectedEvent = Registered {
                VisitId = visitId
                PatientId = patientId
                RegistrationTime = registrationTime
                Address = 
                    {
                        StreetNumber = 123
                        StreetLine1 = "Test St"
                        StreetLine2 = ""
                        Suburb = "Testville"
                        State = "Victoria"
                        Postcode = 3000
                    }
                MedicareNumber = "1234567890"
            }
        
        result = (Success [expectedEvent]) |> should equal true

    [<Fact>]
    let ``Given empty visit When Register Patient Then Patient Registered Event emitted`` () : unit =
        let visitId = VisitId.New()
        let patientId = PatientId.New()
        let registrationTime = DateTime.UtcNow

        let command : RegisterPatientCommand = {
            VisitId = visitId
            PatientId = patientId
            RegistrationTime = registrationTime
            StreetNumber = "123"
            StreetLine1 = "Test St"
            StreetLine2 = ""
            Suburb = "Testville"
            State = "Victoria"
            Postcode = "3000"
            MedicareNumber = "1234567890"
        }

        let result = 
            Visit.registerPatient (Some false) () command

        let expectedEvent : VisitEvents -> bool = function
            | Registered 
                {
                    VisitId = Equals visitId
                    PatientId = Equals patientId
                    RegistrationTime = Equals registrationTime
                } -> true
            | _ -> false
        
        result |> should beSuccessWithEvent<VisitEvents> expectedEvent

    [<Property>]
    let ``All valid command sequences successfully apply to the Read Model`` 
        (visitId : VisitId) =
        Prop.forAll 
            (TestHelpers.getCommandsForAggregate Visit.handlers ("VisitId",visitId)) 
            (fun cmds -> 
                runCommandsAndApplyEventsToViewModel cmds visitId |> ignore
                true)