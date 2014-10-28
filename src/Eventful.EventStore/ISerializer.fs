namespace Eventful.EventStore

open System

type ISerializer = 
    abstract Serialize<'T> : 'T -> byte[]
    abstract DeserializeObj : byte[] -> Type -> obj