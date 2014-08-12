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

        try 
            finalTestSystem.EvaluateState streamName Visit.visitDocumentBuilder |> ignore
            true
        with | e -> false

    let toObjArb<'a> (arb : Arbitrary<'a> ) : Arbitrary<obj> =
        Arb.convert (fun x -> x :> obj) (fun x -> x :?> 'a) arb

    let setField o fieldName value =
        let field = o.GetType().GetField(fieldName, System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Instance)
        field.SetValue(o, value)

    let getArbVisitor = {
         new IRegistrationVisitor<unit,Arbitrary<obj>> with
                member x.Visit<'TCmd> t = Arb.from<'TCmd> |> toObjArb
         }

    let validCommands (aggregateDefinition: AggregateDefinition<_,_,_,_>) visitId : Arbitrary<obj seq> =
        let commandTypes : seq<Arbitrary<obj>> =
            aggregateDefinition.Handlers.CommandHandlers
            |> Seq.map (fun x -> x.Visitable.Receive () getArbVisitor)

        let rec genCommandList (xs : list<obj>) length = gen {
            match length with
            | 0 -> return xs |> List.toSeq
            | _ ->
                let! typeArb = Gen.elements commandTypes
                let! nextCmd = typeArb.Generator
                setField nextCmd "VisitId@" visitId
                return! genCommandList (nextCmd::xs) (length - 1)
        }

        let gen = Gen.sized <| genCommandList []
        let shrink (a:seq<obj>) = 
            let length = a |> Seq.length
            if length = 0 then
                Seq.empty
            else 
                seq {
                    yield a |> Seq.skip 1
                }

        Arb.fromGenShrink (gen, shrink)

    [<Property>]
    let ``All valid command sequences successfully apply to the Read Model`` (visitId : VisitId) =
        Prop.forAll (validCommands Visit.handlers visitId) (fun cmds -> runCommandsAndApplyEventsToViewModel cmds visitId)