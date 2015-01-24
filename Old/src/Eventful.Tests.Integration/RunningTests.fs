﻿namespace Eventful.Tests.Integration

// TODO: REMOVE
//open Xunit
open EventStore.ClientAPI
open System
open System.IO
open System.Net
open Newtonsoft.Json
//open FsUnit.Xunit
open Eventful
open Eventful.EventStore

module RunningTests = 
    let log = createLogger "Eventful.Tests.Integration.RunningTests"

    let settings = new JsonSerializerSettings()
    settings.TypeNameHandling <- TypeNameHandling.All
    let serializer = JsonSerializer.Create(settings)

    let serialize<'T> (t : 'T) =
        use sw = new System.IO.StringWriter() :> System.IO.TextWriter
        serializer.Serialize(sw, t :> obj)
        System.Text.Encoding.UTF8.GetBytes(sw.ToString())

    let deserializeObj (v : byte[]) (objType : Type) : obj =
        let str = System.Text.Encoding.UTF8.GetString(v)
        let reader = new StringReader(str) :> TextReader
        let result = serializer.Deserialize(reader, objType) 
        result

    let esSerializer = 
        { new ISerializer with
            member x.DeserializeObj b t = deserializeObj b t
            member x.Serialize o = serialize o }