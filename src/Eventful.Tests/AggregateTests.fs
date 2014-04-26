namespace Eventful.Tests

open Eventful
open Xunit
open FsUnit.Xunit
open FsCheck
open FsCheck.Xunit
open System

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

type CreatePersonCmd = {
    Id : Guid
    FirstName : string
    LastName : string
}

[<CLIMutable>]
type PersonDocument = {
    Id : Guid
    FirstName : string
    LastName : string
}

module AggregateTests =
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