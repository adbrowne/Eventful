namespace Eventful

// *strongly* inspired by https://github.com/jack-pappas/ExtCore/blob/master/ExtCore/Collections.Bimap.fs
// consider including ExtCore in Eventful
// not doing it right now as it caused weird problems with for * in * do syntax
[<Sealed>]
type Bimap<'Key, 'Value when 'Key : comparison and 'Value : comparison>
    private (map : Map<'Key, 'Value>, inverseMap : Map<'Value, 'Key>) =
    
    /// The empty Bimap instance.
    static let empty = Bimap (Map.empty<'Key,'Value>, Map.empty<'Value,'Key>)
    
    /// The empty Bimap.
    static member Empty
        with get () = empty

    member this.Find key =
        Map.find key map

    member this.FindValue value =
        Map.find value inverseMap

    /// throws if the key or value exist
    member this.AddNew(key,value) =
        if map.ContainsKey key then
            let existingValue = map.Item key
            if(existingValue <> value) then
                failwith <| sprintf "Key already exists: %A" key

        if inverseMap.ContainsKey value then
            let existingKey = inverseMap.Item value
            if(existingKey <> key) then
                failwith <| sprintf "Value already exists: %A" value

        new Bimap<'Key,'Value>(map.Add(key, value), inverseMap.Add(value, key))

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Bimap =
    /// Lookup an element in the map, raising KeyNotFoundException
    /// if no binding exists in the map.
    [<CompiledName("Find")>]
    let inline find key (bimap : Bimap<'Key, 'T>) : 'T =
        bimap.Find key

    /// Lookup a value in the map, raising KeyNotFoundException
    /// if no binding exists in the map.
    [<CompiledName("FindValue")>]
    let inline findValue value (bimap : Bimap<'Key, 'T>) : 'Key =
        bimap.FindValue value

    [<CompiledName("AddNew")>]
    let inline addNew key value (bimap : Bimap<'Key, 'T>) : Bimap<'Key, 'T> =
        bimap.AddNew (key, value)