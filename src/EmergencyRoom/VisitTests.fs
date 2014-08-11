namespace EmergencyRoom

open System
open FsCheck.Xunit
open FsCheck
open FSharpx
open FSharpx.Collections
open Eventful
open Eventful.Testing

module VisitTests = 

    type Marker = Marker 
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

    let invokeGenericMethod (assembly : System.Reflection.Assembly) className methodName genericParameters parameters =
        let arbType = 
            assembly.GetTypes()
            |> Seq.find(fun x -> x.Name = className)
        let m = arbType.GetMethod(methodName).MakeGenericMethod(genericParameters)
        m.Invoke(null, parameters)
        
    let toObjArb<'a> (arb : Arbitrary<'a> ) : Arbitrary<obj> =
        Arb.convert (fun x -> x :> obj) (fun x -> x :?> 'a) arb

    let arbforType (t : Type) =
        let arbForT = invokeGenericMethod typeof<FsCheck.Arbitrary<_>>.Assembly "Arb" "from" [|t|] [||]
        let arbForObj = invokeGenericMethod typeof<Marker>.Assembly "VisitTests" "toObjArb" [|t|] [|arbForT|]
        arbForObj :?> Arbitrary<obj>
    //        printfn "%A" m
    //        Arb.from<int>

    let setField o fieldName value =
        let field = o.GetType().GetField(fieldName, System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Instance)
        field.SetValue(o, value)

    let validCommands (aggregateDefinition: AggregateDefinition<_,_,_,_>) visitId : Arbitrary<obj seq> =
        let commandTypes : seq<Type> =
            aggregateDefinition.Handlers.CommandHandlers
            |> Seq.map (fun x -> x.CmdType)

        let rec genCommandList (xs : list<obj>) length = gen {
            match length with
            | 0 -> return xs |> List.toSeq
            | _ ->
                let! typeToGen = Gen.elements commandTypes
                let! nextCmd = arbforType(typeToGen).Generator
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

    [<Property>]
    let ``Generate type`` () =
        Prop.forAll (arbforType typeof<int>) (fun obj -> printfn "%A" obj)