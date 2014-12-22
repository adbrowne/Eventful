namespace BookLibrary

open System
open Eventful
open FSharpx

[<CLIMutable>]
type NewArrivalsNotificationEvent = 
    { 
        NotificationId : Guid
        BookIds : BookId list
    }
    with interface IEvent

[<CLIMutable>]
type PendingNotificationEvent = 
    { 
        NotificationId : Guid
        BookId : BookId
    }
    with interface IEvent

module NewArrivalsNotification = 
    let notificationId = Guid.Parse("74004864-8DF3-4442-AF02-837439B2DFAC")
    let getStreamName () (id : Guid) =
        sprintf "NewArrivalsNotification-%s" <| id.ToString("N")

    let getEventStreamName (_ : BookLibraryEventContext) (id : Guid) =
        sprintf "NewArrivalsNotification-%s" <| id.ToString("N")

    let inline getDeliveryId (a: ^a) _ = 
        (^a : (member DeliveryId: DeliveryId) (a))

    let buildMetadata = 
        Aggregates.emptyMetadata AggregateType.NewArrivalsNotification

    let inline cmdHandler f = 
        BookLibrary.Aggregates.cmdHandler f buildMetadata

    let evtHandlers = 
        seq {
            let onBookAdded () (e : BookAddedEvent) (c : BookLibraryEventContext) =
                { 
                    UniqueId = c.EventId.ToString() 
                    Events = ({ PendingNotificationEvent.BookId = e.BookId; NotificationId = notificationId } :> IEvent, buildMetadata) |> Seq.singleton
                }
           yield AggregateActionBuilder.onEvent (fun (_:BookAddedEvent) _ -> notificationId) StateBuilder.nullStateBuilder onBookAdded
        }

    let pendingNotificationsBuilder = 
        StateBuilder.Empty "PendingNotifications" Map.empty
        |> StateBuilder.aggregateStateHandler (fun (s, e : PendingNotificationEvent, m : BookLibraryEventMetadata) -> s |> Map.add e.BookId (m.EventTime.AddSeconds(5.0))) // delay notification by up to 5 seconds
        |> StateBuilder.aggregateStateHandler (fun (s, e : NewArrivalsNotificationEvent, m : BookLibraryEventMetadata) -> 
            e.BookIds 
            |> List.fold (fun s bookId -> s |> Map.remove bookId) s)

    let getEarliestNotificationTime =
        let earliestTime s _ time = 
            match (s, time) with
            | None, time -> Some time
            | Some currentTime, time when time < currentTime ->
                Some time
            | _ -> s
        Map.fold earliestTime None

    let wakeupBuilder =
        pendingNotificationsBuilder
        |> AggregateStateBuilder.map getEarliestNotificationTime

    let onWakeup (_ : DateTime) (pendingNotifications : Map<BookId, DateTime>) =
        Seq.singleton ({ NewArrivalsNotificationEvent.NotificationId = notificationId; BookIds = pendingNotifications |> Map.toList |> List.map fst } :> IEvent, buildMetadata)

    let handlers =
        Eventful.Aggregate.toAggregateDefinition 
            AggregateType.NewArrivalsNotification 
            BookLibraryEventMetadata.GetUniqueId
            getStreamName 
            getEventStreamName 
            Seq.empty
            evtHandlers
        |> Eventful.Aggregate.withWakeup 
            wakeupBuilder 
            pendingNotificationsBuilder 
            onWakeup

module NewArrivalsNotificationTests =
    open Xunit
    open Eventful.Testing
    open Swensen.Unquote

    let handlers =
        EventfulHandlers.empty BookLibraryEventMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate NewArrivalsNotification.handlers
        |> StandardConventions.addEventTypes [typeof<BookAddedEvent>; typeof<PendingNotificationEvent>; typeof<NewArrivalsNotificationEvent>]

    let emptyTestSystem = TestSystem.Empty (fun m -> {BookLibraryEventContext.Metadata = m.Metadata; EventId = m.EventId }) handlers

    let notificationsSentBuilder =
        StateBuilder.Empty "NotificationsSent" List.empty
        |> StateBuilder.aggregateStateHandler (fun (s, e:NewArrivalsNotificationEvent,_) -> List.append e.BookIds s)

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Notification is sent for BookAddedEvent`` () =
        let bookId = { BookId.Id = Guid.Parse("66A826AA-D36A-4421-ABED-BFF439C115B3")}
        let bookStreamId = sprintf "Book-%s" (bookId.Id.ToString("N"))
        let bookAddedEvent = { BookAddedEvent.BookId = bookId; Title = "Test" } :> IEvent
        let bookAddedEventMetadata =  {
            SourceMessageId = String.Empty
            EventTime = DateTime.UtcNow
            AggregateType = AggregateType.Book
        }

        let afterRun = 
            emptyTestSystem  
            |> TestSystem.injectEvent bookStreamId bookAddedEvent bookAddedEventMetadata
            |> TestSystem.runToEnd

        let notificationStreamName = NewArrivalsNotification.getStreamName () NewArrivalsNotification.notificationId
        let sent = afterRun.EvaluateState notificationStreamName () notificationsSentBuilder

        sent =? [bookId]