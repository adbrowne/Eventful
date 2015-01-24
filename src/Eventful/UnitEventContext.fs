namespace Eventful

open System

// unit event context that implements
// IDisposable. Use this if you don't have
// an event context in your system
type UnitEventContext = UnitEventContext
with 
    interface IDisposable with
        member x.Dispose() = ()