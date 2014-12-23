namespace Eventful.Testing

open Eventful
open FsCheck
open Swensen.Unquote

module ConfigurationValidation = 
    let ``AggregateType values can be round tripped to a string``
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>)
        (aggregateType : 'TAggregateType) =
            let str = handlers.AggregateTypeToString aggregateType
            let fromString = handlers.StringToAggregateType str
            fromString =? aggregateType

    let ``AggregateType equality matches string equality after conversion`` 
        (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>)
        (aggregateType1 : 'TAggregateType)
        (aggregateType2 : 'TAggregateType) =

        let str1 = handlers.AggregateTypeToString aggregateType1
        let str2 = handlers.AggregateTypeToString aggregateType2

        test <@ (aggregateType1 = aggregateType2) = (str1 = str2) @>
        
    // will be a huge to find errors in the eventful configuration
    let ``Check configuration`` (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent,'TAggregateType>) =
        Check.QuickThrowOnFailure (``AggregateType values can be round tripped to a string`` handlers)
        Check.QuickThrowOnFailure (``AggregateType equality matches string equality after conversion`` handlers)