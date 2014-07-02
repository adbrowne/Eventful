// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open Eventful
open EventStore.ClientAPI

type TestType = TestType

[<EntryPoint>]
let main argv = 

    let sw = System.Diagnostics.Stopwatch.StartNew()

    Eventful.Tests.Integration.RavenProjectorTests.``Pump many events at Raven``()

    sw.Stop()

    consoleLog <| sprintf "Program time: %A ms" sw.ElapsedMilliseconds
    System.Console.ReadKey() |> ignore

    0