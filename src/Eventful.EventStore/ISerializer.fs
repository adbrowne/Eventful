namespace Eventful.EventStore

type ISerializer = 
    abstract Serialize<'T> : 'T -> byte[]
    abstract DeserializeObj : byte[] -> string -> obj