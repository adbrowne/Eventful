namespace EmergencyRoom

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx
open FSharpx.Collections
open Eventful.Aggregate
open Eventful.AggregateActionBuilder
open Eventful.Validation

open FSharpx.Choice
open FSharpx.Validation

open EmergencyRoom.Common

type VisitId = { Id : Guid } 
     with 
     static member New () = 
        { 
            Id = (Guid.NewGuid()) 
        }

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

[<CLIMutable>]
type RegisterPatientCommand = {
    VisitId : VisitId
    PatientId : PatientId
    RegistrationTime : DateTime
    StreetNumber : string
    StreetLine1: string
    StreetLine2: string
    Suburb: string
    State: string
    Postcode: string
    MedicareNumber : string
}

type Address = {
    StreetNumber : int
    StreetLine1: string
    StreetLine2: string
    Suburb: string
    State: string
    Postcode: int
}

type PatientRegisteredEvent = {
    VisitId : VisitId
    PatientId : PatientId
    RegistrationTime : DateTime
    Address : Address
    MedicareNumber: string
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
    DischargeTime : DateTime
}

type PatientDischargedEvent = {
    VisitId : VisitId
    DischargeLocation : DischargeLocation
    DischargeTime : DateTime
}

module Visit = 
    type VisitEvents =
    | Triaged of PatientTriagedEvent
    | Registered of PatientRegisteredEvent
    | PickedUp of PatientPickedUpEvent
    | Discharged of PatientDischargedEvent

    type VisitState = {
        Registered : bool
        CurrentTriageLevel : Option<TriageLevel>
        RegistrationTime : Option<DateTime>
        DischargeTime : Option<DateTime>
    }
    with static member Empty = { 
            Registered = false
            CurrentTriageLevel = None
            DischargeTime = None
            RegistrationTime = None }

    let visitStateBuilder = 
        StateBuilder.Empty VisitState.Empty
        |> StateBuilder.addHandler (fun s (e:PatientTriagedEvent) ->
            { s with CurrentTriageLevel = Some e.TriageLevel })
        |> StateBuilder.addHandler (fun s (e:PatientRegisteredEvent) ->
            { s with   
                 Registered = true
                 RegistrationTime = Some e.RegistrationTime })
        |> StateBuilder.addHandler (fun s (e:PatientDischargedEvent) ->
            { s with DischargeTime = Some e.DischargeTime })
        
    let isRegistered = 
        StateBuilder.Empty false
        |> StateBuilder.addHandler (fun s (e:PatientRegisteredEvent) -> true)
        |> NamedStateBuilder.withName "IsRegistered"

    let getStreamName () (visitId : VisitId) =
        sprintf "Visit-%s" <| visitId.Id.ToString("N")

    let buildAddress streetNumber streetLine1 streetLine2 suburb state postcode =
        {
            Address.StreetNumber = streetNumber
            StreetLine1 = streetLine1
            StreetLine2 = streetLine2
            Suburb = suburb
            State = state
            Postcode = postcode
        }

    let validateAddress streetNumber streetLine1 streetLine2 suburb state postcode =
        buildAddress 
            <!> isNumber "StreetNumber" streetNumber 
            <*> hasText "StreetLine1" streetLine1 
            <*> Success streetLine2 
            <*> hasText "Suburb" suburb
            <*> hasText "State" state
            <*> validatePostcode "Postcode" postcode

    let registerPatient isRegistered context (cmd : RegisterPatientCommand) =

        let buildEvent (address:Address) medicareNumber = 
            [Registered {    
                VisitId = cmd.VisitId
                PatientId = cmd.PatientId
                RegistrationTime = cmd.RegistrationTime
                Address = address
                MedicareNumber = medicareNumber
            }]

        let address =
            validateAddress 
                cmd.StreetNumber 
                cmd.StreetLine1 
                cmd.StreetLine2 
                cmd.Suburb 
                cmd.State 
                cmd.Postcode

        if isRegistered = Some true then
            failWithError "Patient is already registered"
        else 
            buildEvent 
                <!> address
                <*> hasLength "MedicareNumber" 10 cmd.MedicareNumber

    let validateCommand (cmd : RegisterPatientCommand) =
       match (registerPatient None () cmd) with
       | Choice1Of2 _ -> Seq.empty
       | Choice2Of2 errors -> errors |> NonEmptyList.toSeq
        
    let cmdHandlers = 
        seq {
           let triagePatient (cmd : TriagePatientCommand) =
               Triaged { 
                   VisitId = cmd.VisitId
                   TriageLevel = cmd.TriageLevel
               }

           yield buildCmdHandler triagePatient

           yield registerPatient
                |> fullHandler isRegistered
                |> buildCmd

           let pickupPatient (cmd : PickUpPatientCommand) =
                PickedUp {    
                    VisitId = cmd.VisitId
                    PickupTime = cmd.PickupTime
                }

           yield buildCmdHandler pickupPatient

           let dischargePatient (cmd : DischargePatientCommand) =
                Discharged {    
                    VisitId = cmd.VisitId
                    DischargeLocation = cmd.DischargeLocation
                    DischargeTime = cmd.DischargeTime
                }

           yield buildCmdHandler dischargePatient
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

    let ensureDocumentExistsWithId doc (evt : VisitEvents) =
        match (doc, evt) with
        | None, Triaged { VisitId = visitId } 
        | None, Registered { VisitId = visitId } 
        | None, PickedUp { VisitId = visitId } 
        | None, Discharged { VisitId = visitId } -> 
            VisitDocument.NewDoc visitId
        | Some doc, _ -> doc 

    let visitDocumentBuilder = 
        StateBuilder<VisitDocument option>.Empty None
        |> StateBuilder.addHandler (fun doc (evt : PatientTriagedEvent) ->
            doc |> Option.getOrElse (VisitDocument.NewDoc evt.VisitId) |> Some)
        |> StateBuilder.addHandler (fun doc (evt : PatientRegisteredEvent) ->
            let doc = doc |> Option.getOrElse (VisitDocument.NewDoc evt.VisitId)
            Some { doc with 
                    PatientId = Some evt.PatientId; 
                    Registered = Some evt.RegistrationTime } )
        |> StateBuilder.addHandler (fun doc (evt : PatientPickedUpEvent) ->
            let doc = doc |> Option.getOrElse (VisitDocument.NewDoc evt.VisitId)
            Some { doc with
                    PickedUp = Some evt.PickupTime;
                    WaitingTime = Some (evt.PickupTime - doc.Registered.Value) } )