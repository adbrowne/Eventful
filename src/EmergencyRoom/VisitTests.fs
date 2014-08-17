namespace EmergencyRoom

open System
open FsCheck.Xunit
open FsCheck
open FSharpx
open FSharpx.Collections
open Eventful
open Eventful.Testing

module VisitTests = 

    let emptyTestSystem =
        EventfulHandlers.empty
        |> EventfulHandlers.addAggregate Visit.handlers
        |> TestSystem.Empty

    let runCommandsAndApplyEventsToViewModel (cmds : seq<obj>) (visitId : VisitId) =
        let streamName = Visit.getStreamName () visitId
        let applyCommand = flip TestSystem.runCommand
        let finalTestSystem = cmds |> Seq.fold applyCommand emptyTestSystem

    [<Property>]
    let ``All valid command sequences successfully apply to the Read Model`` 
        (visitId : VisitId) =
        Prop.forAll 
            (TestHelpers.getCommandsForAggregate Visit.handlers ("VisitId",visitId)) 
            (fun cmds -> runCommandsAndApplyEventsToViewModel cmds visitId)