namespace EmergencyRoom

open System
open Microsoft.FSharp.Core
open Eventful
open FSharpx
open FSharpx.Collections
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

    type WaitQueueState = {
        QueueStartTimes : Map<PatientId, DateTime>
    }
    with static member Empty = { 
            QueueStartTimes = Map.empty }

    let queueStartTimesBuilder = 
        StateBuilder.Empty WaitQueueState.Empty
        |> StateBuilder.addHandler (fun s (e:PatientAddedToQueueEvent, c:EmergencyEventMetadata) ->
            { s with QueueStartTimes = s.QueueStartTimes |> Map.add e.PatientId c.EventTime })
        |> StateBuilder.addHandler (fun s (e:PatientRemovedFromQueueEvent, _) ->
            { s with QueueStartTimes = s.QueueStartTimes |> Map.remove e.PatientId })
        
    let getStreamName () _ = "WaitQueue"

    let evtHandlers = 
        seq {
           let onPatientRegistered (evt : PatientRegisteredEvent) = seq {
               let context = Common.emptyMetadata()
               yield AddedToQueue {
                    PatientId = evt.PatientId
               }, context }

           let handler = Eventful.AggregateActionBuilder.onEvent Common.systemConfiguration (fun (e:PatientRegisteredEvent) -> getStreamName () e) Common.stateBuilder onPatientRegistered
           yield handler
        }

    let handlers =
        toAggregateDefinition getStreamName getStreamName Seq.empty evtHandlers