namespace BookLibrary

open System
open Eventful
open BookLibrary.Aggregates

type AcceptDeliveryCommand = {
    DeliveryId : DeliveryId
    FileId : FileId
}

module Delivery =
    let getStreamName () (deliveryId : DeliveryId) =
        sprintf "Delivery-%s" <| deliveryId.Id.ToString("N")

    let getEventStreamName (context : UnitEventContext) (deliveryId : DeliveryId) =
        sprintf "Delivery-%s" <| deliveryId.Id.ToString("N")

    let inline getDeliveryId (a: ^a) _ = 
        (^a : (member DeliveryId: DeliveryId) (a))

    let getDeliveryIdFromMetadata = (fun (x : BookLibraryEventMetadata) -> { DeliveryId.Id = x.AggregateId })

    let inline buildDeliveryMetadata (deliveryId : DeliveryId) = 
        Aggregates.emptyMetadata deliveryId.Id AggregateType.Delivery

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
        Eventful.Aggregate.toAggregateDefinition 
            AggregateType.Delivery 
            BookLibraryEventMetadata.GetUniqueId
            getDeliveryIdFromMetadata
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            Seq.empty