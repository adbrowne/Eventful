namespace Eventful

open System 

type WakeupFold<'TMetadata> = IStateBuilder<UtcDateTime option, 'TMetadata, unit>

module Wakeup = 
    let noWakeup<'TAggregateId, 'TMetadata when 'TAggregateId : equality> = 
        AggregateStateBuilder.constant None :> WakeupFold<'TMetadata>