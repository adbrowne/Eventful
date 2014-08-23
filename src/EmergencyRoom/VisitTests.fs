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
        cmds |> Seq.fold applyCommand emptyTestSystem

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
        let run = (fun cmds -> 
                runCommandsAndApplyEventsToViewModel cmds visitId |> ignore
                true)
        Prop.forAll 
            (TestHelpers.getCommandsForAggregate Visit.handlers ("VisitId",visitId)) 
            run

    [<Fact>]
    let ``too many events?`` () : unit =
        let visitId = { Id = Guid.NewGuid() }
        let patientId = { PatientId.Id = Guid.NewGuid() }
        let cmds : List<obj> = 
                                [ {DischargePatientCommand.VisitId = visitId; DischargeLocation = Transfer} :> obj;
                                  { PickUpPatientCommand.VisitId = visitId; PickupTime = DateTime.Parse("5/21/2056 6:51:17 AM")} :> obj
                                  { TriagePatientCommand.VisitId = visitId; TriageLevel = TriageLevel.Level2 } :> obj
                                  { RegisterPatientCommand.VisitId = visitId;
                                    PatientId = patientId
                                    RegistrationTime = DateTime.Parse("10/13/1911 10:47:55 AM")
                                    StreetNumber = "F\xB";
                                    StreetLine1 = "!	G'C\x14?	d";
                                    StreetLine2 = "";
                                    Suburb = "\x16C_c8%";
                                    State = "C\x8~ l1'I?ZP";
                                    Postcode = "r7";} :> obj; 
                                  { RegisterPatientCommand.VisitId = visitId;
                                    PatientId = patientId
                                    RegistrationTime = DateTime.Parse("6/26/2066 6:56:30 PM")
                                    StreetNumber = "\x1C\x12-#h|\x6Es\x1E";
                                    StreetLine1 = "Km\!_";
                                    StreetLine2 = "&rZ\x18x\x14e\x10";
                                    Suburb = "UpQ";
                                    State = "Sg\x1404	";
                                    Postcode = "";} :> obj;
                                  { TriagePatientCommand.VisitId = visitId; TriageLevel = TriageLevel.Level2 } :> obj
                                  { TriagePatientCommand.VisitId = visitId; TriageLevel = TriageLevel.Level2 } :> obj
                                  {DischargePatientCommand.VisitId = visitId; DischargeLocation = Transfer} :> obj;
                                  { PickUpPatientCommand.VisitId = visitId; PickupTime = DateTime.Parse("5/21/2056 6:51:17 AM")} :> obj
                                  { TriagePatientCommand.VisitId = visitId; TriageLevel = TriageLevel.Level2 } :> obj
                                  {DischargePatientCommand.VisitId = visitId; DischargeLocation = Transfer} :> obj;
//                                  { RegisterPatientCommand.VisitId = visitId;
//                                    PatientId = patientId
//                                    RegistrationTime = DateTime.Parse("6/26/2066 6:56:30 PM")
//                                    StreetNumber = "\x1C\x12-#h|\x6Es\x1E";
//                                    StreetLine1 = "Km\!_";
//                                    StreetLine2 = "&rZ\x18x\x14e\x10";
//                                    Suburb = "UpQ";
//                                    State = "Sg\x1404	";
//                                    Postcode = "r7";} :> obj;
//                                  { RegisterPatientCommand.VisitId = visitId;
//                                    PatientId = patientId
//                                    RegistrationTime = DateTime.Parse("6/26/2066 6:56:30 PM")
//                                    StreetNumber = "\x1C\x12-#h|\x6Es\x1E";
//                                    StreetLine1 = "Km\!_";
//                                    StreetLine2 = "&rZ\x18x\x14e\x10";
//                                    Suburb = "UpQ";
//                                    State = "Sg\x1404	";
//                                    Postcode = "";} :> obj;
                                  { PickUpPatientCommand.VisitId = visitId; PickupTime = DateTime.Parse("5/21/2056 6:51:17 AM")} :> obj]
        runCommandsAndApplyEventsToViewModel cmds visitId |> ignore

