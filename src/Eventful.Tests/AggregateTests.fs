namespace Eventful.Tests

open Eventful
open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open System
open FSharpx.Choice
open EkonBenefits.FSharp.Dynamic

type PersonCreatedEvt = {
    Id : Guid
    FirstName : string
    LastName : string
}

type Eventful (aggregates : Aggregate<'TResult> seq, readModel : ReadModel) =
    member this.RunCommand<'TCmd> (cmd:'TCmd) = 
        let (handler, agg) = 
            aggregates 
            |> Seq.toList
            |> List.map (fun agg -> agg.GetCommandHandlers cmd)
            |> List.collect (function 
                            | Some handler -> [handler]
                            | None -> [])
            |> function
            | [handler] -> handler
            | [] -> failwith <| sprintf "No handlers found for command: %A" typeof<'TCmd>
            | _ -> failwith <| sprintf "Multiple handlers found for command: %A" typeof<'TCmd>

        let state = agg.GetZeroState()
        let result = handler.Invoke state cmd

        let events = 
            match result with
            | Choice1Of2 events -> events
            | _ -> []

        events
        |> List.iter readModel.RunEvent

type InMemoryDocumentStore () =
    let mutable documents : Map<string, obj> = Map.empty
    interface IDocumentStore with
        member this.GetDocument<'TDocument> key =
            match documents |> Map.containsKey key with
            | true -> Some (documents.[key] :?> 'TDocument)
            | false -> None
        member this.Store<'TDocument> key (document : 'TDocument) =
            documents <- documents |> Map.add key (document :> obj)
            ()

[<CLIMutable>]
type PersonDocument = {
    Id : Guid
    FirstName : string
    LastName : string
}

[<Sealed>]
type CommandHandlerAttribute () =
    class
        inherit System.Attribute ()
    end

[<Sealed>]
type AggregateModuleAttribute () =
    class
        inherit System.Attribute ()
    end

type CommandError =
    | PropertyValidation of (string * string)
    | CommandValidation of (string)

type CommandResult = Choice<obj list, CommandError list>

type CommandHandlerResult<'TCmd, 'TState> = {
   GetId: Guid 
   Cmd: 'TCmd
   State: 'TState
   Run: unit -> CommandResult
}

module Handler =
    let AmmendRun f (current : CommandHandlerResult<_,_>) =
        { current with Run = (fun () -> current.Run () |> f) }

    let Start aggregateId cmd state =
        { 
            GetId = aggregateId
            Cmd = cmd
            State = state
            Run = fun () -> Choice1Of2 []
        }
    let Output x (current : CommandHandlerResult<_,_>) = 
        AmmendRun (FSharpx.Choice.map (fun xs -> (x :> obj)::xs)) current

module Validate =
    let validator pred error x (current : CommandHandlerResult<_,_>) =
        if pred x then current
        else
            current 
            |> Handler.AmmendRun (function
                | Choice1Of2 value -> Choice2Of2 [error]
                | Choice2Of2 xs -> Choice2Of2 (error::xs))

    let NonNullProperty propertyName = validator ((<>) null) (PropertyValidation (propertyName, sprintf "%s is null" propertyName))

[<AggregateModule>]
module PersonAggregate = 
    type internal Marker = interface end

    type PersonState = {
        FirstName : string
        LastName : string
    }

    let state = new StateGen<PersonState>((fun s _ -> s), { PersonState.FirstName = ""; LastName = "" }) 

    type CreatePersonCmd = {
        Id : Guid
        FirstName : string
        LastName : string
    }

    [<CommandHandler>]
    let HandleCreatePerson (cmd : CreatePersonCmd, state : PersonState) =
        Handler.Start cmd.Id cmd state
        |> Validate.NonNullProperty "FirstName" cmd.FirstName
        |> Validate.NonNullProperty "LastName" cmd.LastName
        |> Handler.Output 
            {
                PersonCreatedEvt.Id = cmd.Id
                FirstName = cmd.FirstName
                LastName = cmd.LastName
            }

module AggregateTests =
    [<Fact>]
    let ``Test validation`` () : unit =
        let cmd = {
                PersonAggregate.CreatePersonCmd.Id = Guid.NewGuid()
                PersonAggregate.CreatePersonCmd.FirstName = null
                PersonAggregate.CreatePersonCmd.LastName = null
            }
        let state = { PersonAggregate.PersonState.FirstName = ""; PersonAggregate.PersonState.LastName = ""}
        let result = PersonAggregate.HandleCreatePerson (cmd, state)
        let runResult = result.Run ()
        printfn "Result %A" result
        printfn "RunResult %A" runResult

    let hasAttribute (attribute : Type) (m : System.Reflection.MethodInfo) =
        not (m.GetCustomAttributes(attribute,true) |> Array.isEmpty)

    let invoke2 (m : System.Reflection.MethodInfo) (state : obj) (cmd : obj) =
        let cmdResult = m.Invoke(null, [|cmd; state|])
        cmdResult?Run?Invoke null
//
    let invokeGenericCommandHandler cmdType stateType (instance : 'T) methodName (actualMethod : System.Reflection.MethodInfo) =
        let methodInfo = instance.GetType().GetMethod(methodName)
//        let paramType = Microsoft.FSharp.Reflection.FSharpType.MakeTupleType([|cmdType;stateType|])
        // let myMethod = actualMethod.MakeGenericMethod([|cmdType|])
//        // let myMethod2 = myMethod.MakeGenericMethod([|stateType|])
        let param : (obj -> obj -> CommandResult) = (fun (state : obj) (cmd : obj) -> invoke2 actualMethod state cmd)
        methodInfo.Invoke(instance, [|stateType;cmdType;param|]) :?> 'T

    let aggregateModuleToAggregate (moduleType : Type) : Aggregate<CommandResult> =
        let commandHandlers = 
            moduleType.GetMethods()
            |> Seq.ofArray
            |> Seq.filter (hasAttribute typeof<CommandHandlerAttribute>)
            |> Seq.map (fun (m : System.Reflection.MethodInfo) -> (m.GetParameters(), m))
            |> Seq.map (fun ([|cmdParamInfo; stateParamInfo|], m) -> (cmdParamInfo.ParameterType, stateParamInfo.ParameterType, m))
            // |> Seq.map (fun (cmdType, stateType, m) -> (cmdType, stateType, ))

        let aggregateWithCmdHandlers = 
            commandHandlers 
            |> Seq.fold (fun (agg : Aggregate<_>) (cmdType, stateType, cmd) -> invokeGenericCommandHandler cmdType stateType agg "AddUntypedCmdHandler" cmd) Aggregate.Empty

        aggregateWithCmdHandlers.AddStateGenerator PersonAggregate.state

    let assemblyToAggregates (assembly : System.Reflection.Assembly) =
        assembly.GetExportedTypes()
        |> Seq.ofArray
        |> Seq.map (fun (t : Type) -> ((t.GetCustomAttributes(typeof<AggregateModuleAttribute>, true) |> Array.length), t))
        |> Seq.filter (fun (l,_) -> l > 0)
        |> Seq.map (fun (_,t) -> aggregateModuleToAggregate t)

    open PersonAggregate
    [<Fact>]
    let ``Create aggregate from module`` () : unit =
       printfn "%A" (assemblyToAggregates (typeof<PersonAggregate.Marker>.Assembly))


    [<Fact>]
    let ``Can call my thing`` () : unit =
        let cmd = {
            PersonAggregate.CreatePersonCmd.Id = Guid.NewGuid()
            PersonAggregate.CreatePersonCmd.FirstName = null
            PersonAggregate.CreatePersonCmd.LastName = null
        }
        let state = { PersonAggregate.PersonState.FirstName = ""; PersonAggregate.PersonState.LastName = ""}

        let moduleType =
            typeof<PersonAggregate.Marker>.Assembly.GetExportedTypes()
            |> Seq.ofArray
            |> Seq.map (fun (t : Type) -> ((t.GetCustomAttributes(typeof<AggregateModuleAttribute>, true) |> Array.length), t))
            |> Seq.filter (fun (l,_) -> l > 0)
            |> Seq.map (fun (_,t) -> t)
            |> Seq.head
        let staticContext = EkonBenefits.FSharp.Dynamic.dynStaticContext(moduleType)
        let f : obj -> obj -> CommandResult = (fun a b -> 
            let r1 = staticContext?HandleCreatePerson(a,b)
            // typeof<CommandHandlerResult<_,_>>.GetMethod("Run").Invoke(r1, 
            let r2 = r1?Run?Invoke null
            r2)
        printfn "%A" (f cmd state)

    [<Fact>]
    let ``Creating a person results in a person document`` () : unit =
        let processPersonCmd _ (cmd : CreatePersonCmd) =
            {
                PersonCreatedEvt.Id = cmd.Id
                FirstName = cmd.FirstName
                LastName = cmd.LastName
            }

        let processPersonEvt (doc : PersonDocument) (evt : PersonCreatedEvt) =
            { doc with
                    Id = evt.Id
                    FirstName = evt.FirstName
                    LastName = evt.LastName }

        let getPersonId (evt : PersonCreatedEvt) = "Person/" + evt.Id.ToString()

        let personAggregate = Aggregate.Empty.AddCmdHandler(processPersonCmd)
        let aggregates = assemblyToAggregates (typeof<PersonAggregate.Marker>.Assembly)
        let documentStore : IDocumentStore = new InMemoryDocumentStore() :> IDocumentStore

        let emptyReadModel = (ReadModel.Empty documentStore)
        let readModel = emptyReadModel.Add getPersonId processPersonEvt
        let eventful = new Eventful(aggregates, readModel)

        let id = Guid.NewGuid()

        eventful.RunCommand {
                CreatePersonCmd.Id = id
                FirstName = "Andrew"            
                LastName = "Browne"
            }

//        outEvent.Id |> should equal id
//        outEvent.FirstName |> should equal "Andrew"
//        outEvent.LastName |> should equal "Browne"

        let (Some (andrewDoc : PersonDocument))  = documentStore.GetDocument("Person/" + id.ToString())
        andrewDoc |> should not' (be Null)

        andrewDoc.Id |> should equal id
        andrewDoc.FirstName |> should equal "Andrew"
        andrewDoc.LastName |> should equal "Browne"