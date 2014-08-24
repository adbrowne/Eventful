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
        if(cmds |> Seq.length > 100) then
            failwith "cmds too long"

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
            emptyTestSystem
            |> TestSystem.runCommand command

        let expectedEvent : PatientRegisteredEvent = {
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
        
        let lastEvent = 
            result.LastResult 
            |> Choice.map (List.map (fun (_,evt,_) -> evt)) // ignore metadata
        lastEvent = (Choice1Of2 [expectedEvent]) |> should equal true

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
            emptyTestSystem
            |> TestSystem.runCommand command

        let expectedEvent : PatientRegisteredEvent -> bool = function
            | {
                    VisitId = Equals visitId
                    PatientId = Equals patientId
                    RegistrationTime = Equals registrationTime
              } -> true
            | _ -> false
        
        let lastEvent = 
            result.LastResult 
            |> Choice.map (List.map (fun (_,evt,_) -> evt)) // ignore metadata
        result.LastResult |> should beSuccessWithEvent<PatientRegisteredEvent, EmergencyEventMetadata> expectedEvent

    [<Property>]
    let ``All valid command sequences successfully apply to the Read Model`` 
        (visitId : VisitId) =
        Prop.forAll 
            (TestHelpers.getCommandsForAggregate Visit.handlers ("VisitId",visitId)) 
            (fun cmds -> 
                runCommandsAndApplyEventsToViewModel cmds visitId |> ignore
                true)