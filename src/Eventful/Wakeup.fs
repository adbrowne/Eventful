namespace Eventful

open System 

type WakeupFold<'TMetadata> = EventFold<DateTime option, 'TMetadata, unit>

module Wakeup = 
    let noWakeup<'TMetadata> : EventFold<DateTime option, 'TMetadata, unit> = 
        EventFold.Empty None