namespace BookLibrary

open System
open System.IO
open Eventful
open Newtonsoft.Json

module Serialization = 
    let serializer = JsonSerializer.Create()
    serializer.Converters.Add(new Converters.StringEnumConverter())

    let serialize (t : 'T) =
        use sw = new System.IO.StringWriter() :> System.IO.TextWriter
        serializer.Serialize(sw, t :> obj)
        System.Text.Encoding.UTF8.GetBytes(sw.ToString())

    let deserializeObj (v : byte[]) (objType : Type) : obj =
        let str = System.Text.Encoding.UTF8.GetString(v)
        if objType = typeof<string> then
            str :> obj
        elif objType.IsEnum then
            Enum.Parse(objType, str, true)
        else
            let reader = new StringReader(str) :> TextReader
            let result = serializer.Deserialize(reader, objType)
            result

    let esSerializer = 
        { new ISerializer with
            member x.DeserializeObj b t = deserializeObj b t
            member x.Serialize o = serialize o }