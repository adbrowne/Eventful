namespace EmergencyRoom

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx
open Eventful.Aggregate

type VisitId = { Id : Guid } 

type TriageLevel = 
| Level1
| Level2
| Level3
| Level4
| Level5
| DOA

type Ward = 
| Ward1
| Ward2
| Ward3
| Ward4

type DischargeLocation = 
| Home
| Ward of Ward
| Transfer

type TriagePatientCommand = {
    VisitId : VisitId
    TriageLevel : TriageLevel
}

type PatientTriagedEvent = {
    VisitId : VisitId
    TriageLevel : TriageLevel
}

type RegisterPatientCommand = {
    VisitId : VisitId
    PatientId : PatientId
    RegistrationTime : DateTime
}

type PatientRegisteredEvent = {
    VisitId : VisitId
    PatientId : PatientId
    RegistrationTime : DateTime
}

type PickUpPatientCommand = {
    VisitId : VisitId
    PickupTime : DateTime
}

type PatientPickedUpEvent = {
    VisitId : VisitId
    PickupTime : DateTime
}

type DischargePatientCommand = {
    VisitId : VisitId
    DischargeLocation : DischargeLocation
}

type PatientDischaredEvent = {
    VisitId : VisitId
    DischargeLocation : DischargeLocation
}

open Eventful.AggregateActionBuilder
open Eventful.Validation

module Visit = 
    type VisitEvents =
    | Triaged of PatientTriagedEvent
    | Registered of PatientRegisteredEvent
    | PickedUp of PatientPickedUpEvent
    | Discharged of PatientDischaredEvent

    let stateBuilder = NamedStateBuilder.nullStateBuilder

    let getStreamName () (visitId : VisitId) =
        sprintf "Visit-%s" <| visitId.Id.ToString("N")

    let cmdHandlers = 
        seq {
           let triagePatient (cmd : TriagePatientCommand) =
               Triaged { 
                   VisitId = cmd.VisitId
                   TriageLevel = cmd.TriageLevel
               }

           yield triagePatient
                 |> simpleHandler stateBuilder
                 |> buildCmd

           let registerPatient (cmd : RegisterPatientCommand) =
                Registered {    
                    VisitId = cmd.VisitId
                    PatientId = cmd.PatientId
                    RegistrationTime = cmd.RegistrationTime
                }

           yield registerPatient
                |> simpleHandler stateBuilder
                |> buildCmd

           let pickupPatient (cmd : PickUpPatientCommand) =
                PickedUp {    
                    VisitId = cmd.VisitId
                    PickupTime = cmd.PickupTime
                }

           yield pickupPatient
                |> simpleHandler stateBuilder
                |> buildCmd

           let dischargePatient (cmd : DischargePatientCommand) =
                Discharged {    
                    VisitId = cmd.VisitId
                    DischargeLocation = cmd.DischargeLocation
                }

           yield dischargePatient
                |> simpleHandler stateBuilder
                |> buildCmd
        }

    let handlers =
        toAggregateDefinition getStreamName getStreamName cmdHandlers Seq.empty

    type VisitDocument = {
        VisitId : VisitId
        PatientId : PatientId option
        Registered : DateTime option
        PickedUp : DateTime option
        WaitingTime : TimeSpan option
    }
    with static member NewDoc visitId = {
            VisitId = visitId
            PatientId = None
            Registered = None
            PickedUp = None
            WaitingTime = None
        }

    let visitDocumentBuilder = 
        StateBuilder<VisitDocument option>.Empty None
        |> StateBuilder.addHandler (fun doc (evt : VisitEvents) ->
            let docWithId = 
                match (doc, evt) with
                | None, Triaged { VisitId = visitId } 
                | None, Registered { VisitId = visitId } 
                | None, PickedUp { VisitId = visitId } 
                | None, Discharged { VisitId = visitId } -> 
                    VisitDocument.NewDoc visitId
                | Some doc, _ -> doc

            match evt with
            | Registered evtDetails ->
                Some { docWithId with 
                        PatientId = Some evtDetails.PatientId; 
                        Registered = Some evtDetails.RegistrationTime } 
            | PickedUp { PickupTime = pickupTime } ->
                Some { docWithId with
                        PickedUp = Some pickupTime;
                        WaitingTime = Some (pickupTime - docWithId.Registered.Value) }
            | _ -> Some docWithId
        )