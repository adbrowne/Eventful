namespace Eventful.Tests.Folding

open Xunit
open System
open FsUnit.Xunit
open Eventful


module FoldCombining = 
    type CustomerId = {
        Id : Guid
    }
    with 
        static member New () = 
            { Id = Guid.NewGuid() }

    type OrderId = {
        Id : Guid
    }
    with 
        static member New () = 
            { Id = Guid.NewGuid() }

    type ItemId = {
        Id : Guid
    }
    with 
        static member New () = 
            { Id = Guid.NewGuid() }

    type OrderCreatedEvent = {
        CustomerId : CustomerId
        OrderId : OrderId
    }

    type OrderItemAddedEvent = {
        OrderId : OrderId
        ItemId : ItemId
        Quantity : int
        Note : string option
    }

    type OrderItemNoteSetEvent = {
        OrderId : OrderId
        ItemId : ItemId
        Note : string
    }

    type OrderItemRemovedEvent = {
        OrderId : OrderId
        ItemId : ItemId
        Quantity : int
    }

    type OrderCancelledEvent = {
        OrderId : OrderId
    }

    type OrderPaidEvent = {
        OrderId : OrderId
        Amount : Decimal
    }

    type OrderStatus =
        | Created
        | ItemsAdded
        | Paid
        | Cancelled

    type ItemState = {
        Quantity : int
        Note : string option
    }

    let orderStateBuilder =
        StateBuilder.Empty OrderStatus.Created
        |> StateBuilder.addHandler (fun s (e:OrderItemAddedEvent) -> OrderStatus.ItemsAdded)
        |> StateBuilder.addHandler (fun s (e:OrderPaidEvent) -> OrderStatus.Paid)
        |> StateBuilder.addHandler (fun s (e:OrderCancelledEvent) -> OrderStatus.Cancelled)

    let orderItemCountBuilder =
        StateBuilder.Empty 0
        |> StateBuilder.addHandler (fun count (e:OrderItemAddedEvent) -> count + e.Quantity)
        |> StateBuilder.addHandler (fun count (e:OrderItemRemovedEvent) -> count - e.Quantity)

    let orderItemNoteBuilder =
        StateBuilder.Empty None
        |> StateBuilder.addHandler (fun s (e:OrderItemAddedEvent) -> match e.Note with
                                                                     | Some v -> Some v
                                                                     | None -> s)
        |> StateBuilder.addHandler (fun _ (e:OrderItemNoteSetEvent) -> Some e.Note)

    let itemIdMapper =
        IdMapper.Empty
        |> IdMapper.addHandler (fun (e:OrderItemAddedEvent) -> e.ItemId)
        |> IdMapper.addHandler (fun (e:OrderItemNoteSetEvent) -> e.ItemId)
        |> IdMapper.addHandler (fun (e:OrderItemRemovedEvent) -> e.ItemId)

    let idMapper =
        IdMapper.Empty
        |> IdMapper.addHandler (fun (e:OrderItemAddedEvent) -> e.OrderId)
        |> IdMapper.addHandler (fun (e:OrderItemNoteSetEvent) -> e.OrderId)
        |> IdMapper.addHandler (fun (e:OrderItemRemovedEvent) -> e.OrderId)
            
    let orderItemStateBuilder =
        StateBuilder.map2 
            (fun count note -> { Quantity = count; Note = note }) 
            (fun { Quantity = count; Note = note } -> (count, note))
            orderItemCountBuilder
            orderItemNoteBuilder

    let orderItemStateByItem =
        ChildStateBuilder.BuildWithMagicMapper<ItemId> orderItemStateBuilder

    let orderItemStateMap =
        StateBuilder.toMap orderItemStateByItem

    type AggregateState = {
        State : OrderStatus
        OrderItemDetails : Map<ItemId,ItemState>
    }

    let orderAggregateStateBuilder =
        let combine state itemDetails = { State = state; OrderItemDetails = itemDetails }
        let extract { State = state; OrderItemDetails = itemDetails } = (state, itemDetails)
        StateBuilder.map2 combine extract orderStateBuilder orderItemStateMap

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can combine stateBuilders`` () : unit = 
        let orderId = OrderId.New()
        let itemId = ItemId.New()
        let events : obj list = [
            { OrderItemAddedEvent.OrderId = orderId; ItemId = itemId; Quantity = 1; Note = Some "initial note" }
            { OrderItemNoteSetEvent.OrderId = orderId; ItemId = itemId; Note = "note update" }
            { OrderItemAddedEvent.OrderId = orderId; ItemId = itemId; Quantity = 2; Note = None }
            { OrderItemRemovedEvent.OrderId = orderId; ItemId = itemId; Quantity = 1; }
        ]
        let result = StateBuilder.runState orderItemStateBuilder events
        result |> should equal { Note = Some "note update"; Quantity = 2 }

    let groupByOrderId : StateBuilder<Map<ItemId,ItemState>> =
        ChildStateBuilder.BuildWithMagicMapper<ItemId> orderItemStateBuilder
        |> StateBuilder.toMap

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can create map by id`` () : unit = 
        let orderId = OrderId.New()
        let itemId = ItemId.New()
        let itemId2 = ItemId.New()

        let events : obj list = [
            { OrderItemAddedEvent.OrderId = orderId; ItemId = itemId; Quantity = 1; Note = Some "initial note" }
            { OrderItemNoteSetEvent.OrderId = orderId; ItemId = itemId; Note = "note update" }
            { OrderItemAddedEvent.OrderId = orderId; ItemId = itemId2; Quantity = 2; Note = None }
            { OrderItemRemovedEvent.OrderId = orderId; ItemId = itemId; Quantity = 1; }
        ]
        let result = StateBuilder.runState orderItemStateMap events
        let expected =
            Map.empty 
            |> Map.add itemId { Note = Some "note update"; Quantity = 0 }
            |> Map.add itemId2 { Note = None; Quantity = 2 }

        result |> should equal expected