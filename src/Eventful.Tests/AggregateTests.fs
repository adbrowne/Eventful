namespace Eventful.Tests

open Eventful
open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open System
open FSharpx.Choice

type PersonCreatedEvt = {
    Id : Guid
    FirstName : string
    LastName : string
}

type Eventful (aggregates : Aggregate<'TResult> seq, readModel : ReadModel) =
    member this.RunCommand<'TCmd> (cmd:'TCmd) = 
        let handler = 
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

        let result = handler.Invoke (new Object()) cmd

        readModel.RunEvent result

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

type CommandError =
    | PropertyValidation of (string * string)
    | CommandValidation of (string)

type CommandResult = Choice<obj list, CommandError list>

type CommandHandlerResult<'TCmd, 'TState> = {
   GetId: Guid 
   Cmd: 'TCmd
   StateGen: StateGen<'TState>
   Run: 'TState -> CommandResult
}

module Handler =
    let AmmendRun f (current : CommandHandlerResult<_,_>) =
        { current with Run = (fun state -> current.Run state |> f) }

    let Start aggregateId cmd (stateGen : StateGen<'TState>)=
        { 
            GetId = aggregateId
            Cmd = cmd
            StateGen = stateGen
            Run = fun (state : 'TState) -> Choice1Of2 []
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

module PersonAggregate = 
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
    let HandleCreatePerson (cmd : CreatePersonCmd) =
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
        let result = 
            PersonAggregate.HandleCreatePerson {
                Id = Guid.NewGuid()
                FirstName = null
                LastName = null
            }
        let runResult = result.Run result.StateGen.Zero
        printfn "Result %A" result
        printfn "RunResult %A" runResult

    open PersonAggregate
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
        let documentStore : IDocumentStore = new InMemoryDocumentStore() :> IDocumentStore

        let emptyReadModel = (ReadModel.Empty documentStore)
        let readModel = emptyReadModel.Add getPersonId processPersonEvt
        let eventful = new Eventful(Seq.singleton personAggregate, readModel)

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