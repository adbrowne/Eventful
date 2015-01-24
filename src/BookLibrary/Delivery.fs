namespace BookLibrary

open Eventful
open BookLibrary.Aggregates

type AcceptDeliveryCommand = {
    DeliveryId : DeliveryId
    FileId : FileId
}

module Delivery =
    let getStreamName () (deliveryId : DeliveryId) =
        sprintf "Delivery-%s" <| deliveryId.Id.ToString("N")

    let getEventStreamName (context : BookLibraryEventContext) (deliveryId : DeliveryId) =
        sprintf "Delivery-%s" <| deliveryId.Id.ToString("N")

    let inline getDeliveryId (a: ^a) _ = 
        (^a : (member DeliveryId: DeliveryId) (a))

    let buildDeliveryMetadata = 
        Aggregates.emptyMetadata AggregateType.Delivery

    let inline deliveryCmdHandler f = 
        cmdHandler f buildDeliveryMetadata

    let cmdHandlers = 
        seq {
           let addDelivery (cmd : AcceptDeliveryCommand) =
               { 
                   DeliveryAcceptedEvent.DeliveryId = cmd.DeliveryId
                   FileId = cmd.FileId
               }

           yield deliveryCmdHandler addDelivery
        }

    let handlers () =
        Aggregates.toAggregateDefinition 
            AggregateType.Delivery 
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            Seq.empty

open System
open Suave
open Suave.Http
open Suave.Http.Applicatives
open BookLibrary.WebHelpers

module DeliveryWebApi = 
    let acceptHandler (cmd : AcceptDeliveryCommand) =
        let cmd = { cmd with DeliveryId = { DeliveryId.Id = Guid.NewGuid() }}
        let successResponse = 
            let responseBody = new Newtonsoft.Json.Linq.JObject();
            responseBody.Add("deliveryId", new Newtonsoft.Json.Linq.JValue(cmd.DeliveryId.Id))
            responseBody
        (cmd, successResponse :> obj)

    let config system =
        choose [
            url "/api/delivery" >>= choose
                [ 
                    POST >>= commandHandler system acceptHandler
                ]
        ]