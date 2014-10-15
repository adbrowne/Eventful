namespace EmergencyRoom

open System
open Microsoft.FSharp.Core
open Eventful
open Eventful.Aggregate

type PatientAddedToQueueEvent = {
    PatientId : PatientId
}

type PatientRemovedFromQueueEvent = {
    PatientId : PatientId
}

type PatientWaitingTooLongEvent = {
    PatientId : PatientId
    DurationInMinutes : int
}

module WaitQueue = 
    type WaitQueueEvents =
    | AddedToQueue of PatientAddedToQueueEvent
    | RemovedFromQueue of PatientRemovedFromQueueEvent
    | PatientWaitingTooLong of PatientWaitingTooLongEvent

    let queueAggregateId = Guid.Parse("57495205-c054-4a54-972c-44afffa56b63")
    type WaitQueueState = {
        QueueStartTimes : Map<PatientId, DateTime>
    }
    with static member Empty = { 
            QueueStartTimes = Map.empty }

//    let queueStartTimesBuilder = 
//        StateBuilder.Empty WaitQueueState.Empty
//        |> StateBuilder.addHandler (fun s (e:PatientAddedToQueueEvent, c:EmergencyEventMetadata) ->
//            { s with QueueStartTimes = s.QueueStartTimes |> Map.add e.PatientId c.EventTime })
//        |> StateBuilder.addHandler (fun s (e:PatientRemovedFromQueueEvent, _) ->
//            { s with QueueStartTimes = s.QueueStartTimes |> Map.remove e.PatientId })
        
    let getStreamName () _ = "WaitQueue"

    let inline onEventHandler stateBuilder handler = 
        Eventful.AggregateActionBuilder.onEvent 
            Common.systemConfiguration 
            (fun (e:PatientRegisteredEvent) -> getStreamName () e) 
            stateBuilder 
            handler

    let evtHandlers = 
        seq {
           let onPatientRegistered (evt : PatientRegisteredEvent) = seq {
               let context = Common.emptyMetadata
               yield AddedToQueue {
                    PatientId = evt.PatientId
               }, context }

           yield onEventHandler Common.stateBuilder onPatientRegistered
        }

    let handlers =
        toAggregateDefinition getStreamName getStreamName (fun _ -> queueAggregateId) Seq.empty evtHandlers