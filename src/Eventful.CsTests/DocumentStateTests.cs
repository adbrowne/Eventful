using System;
using System.Collections.Generic;
using System.Linq;
using Eventful.Tests.Folding;

namespace Eventful.CsTests
{
    public class DocumentStateTests
    {
        public class MyOrderDocument
        {
            public MyOrderDocument()
            {
                Items = new List<OrderItem>();
            }

            public FoldCombining.OrderStatus Status { get; set; }
            public List<OrderItem> Items { get; set; }
        }

        public class OrderItem
        {
            public FoldCombining.ItemId ItemId { get; set; }
            public FoldCombining.ItemState State { get; set; }
        }

        public class MyProjector : BaseProjector<MyOrderDocument>
        {
            public MyProjector()
            {
                MapState(FoldCombining.orderStateBuilder, x => x.Status, (x, v) => x.Status = v);
                MapChildState<FoldCombining.ItemState,FoldCombining.ItemId>(
                    FoldCombining.orderItemStateByItem, 
                    (doc, itemId) => GetItem(doc, itemId).State,
                    (doc, itemId, value) => GetItem(doc, itemId).State = value);
            }

            private OrderItem GetItem(MyOrderDocument doc, FoldCombining.ItemId itemId)
            {
                var itemDoc = doc.Items.SingleOrDefault(x => x.ItemId.Equals(itemId));

                if (itemDoc == null)
                {
                    itemDoc = new OrderItem
                        {
                            ItemId = itemId, State = FoldCombining.orderItemStateByItem.InitialState
                        };
                    doc.Items.Add(itemDoc);
                }

                return itemDoc;
            }
        }

        public void CanMapChildStateToDocument()
        {

        }
    }

    public class BaseProjector<T>
    {
        protected void MapState<TState>(StateBuilder<TState> orderStateBuilder, Func<T, TState> getter, Action<T, TState> setter)
        {
        }

        protected void MapChildState<TState, TChildId>(ChildStateBuilder<TState, TChildId> childStateBuilder, Func<T, TChildId, TState> getter, Action<T, TChildId, TState> setter)
        {
            
        }
    }
}