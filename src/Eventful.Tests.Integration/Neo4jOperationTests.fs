namespace Eventful.Tests.Integration

open System

open Xunit
open FsUnit.Xunit
open NHamcrest

open Neo4jClient
open Eventful.Neo4j
open Eventful.Neo4j.Operations

module Neo4jOperationTests =
    let testGraphName = "eventful-test"

    let buildGraphClientAndCleanTestGraph () =
        let uri = new Uri("http://192.168.59.103:7474/db/data")
        let graphClient = new GraphClient(uri)
        graphClient.Connect()

        let query = beginQ graphClient testGraphName
        
        query
        |> matchQ (sprintf "(n:%s)-[r]-()" (graphLabel query))
        |> deleteQ "r, n"
        |> executeQ

        graphClient

    let shouldBeSuccessful = function
        | Choice2Of2 error -> raise error
        | _ -> ()

    let relationship = { From = "a"; To = "b"; Type = "test" }
    
    let countRelationship graphClient : int =
        let query = beginQ graphClient testGraphName
        let query, fromSelector = query |> withNodeSelectorQ "from" relationship.From
        let query, toSelector = query |> withNodeSelectorQ "to" relationship.To

        query
        |> matchQ (sprintf "%s-[:%s]->%s" fromSelector relationship.Type toSelector)
        |> returnQ "count(*)"
        |> resultsQ
        |> Seq.exactlyOne

    let runBatch graphClient batch =
        writeBatch graphClient testGraphName [ batch ]
        |> Async.RunSynchronously
        |> shouldBeSuccessful

    [<Fact>]
    let ``Can add relationship`` () : unit =
        let graphClient = buildGraphClientAndCleanTestGraph ()

        countRelationship graphClient |> should equal 0
        runBatch graphClient [ GraphTransaction [ AddRelationship relationship ] ]
        countRelationship graphClient |> should equal 1

    [<Fact>]
    let ``Can remove relationship`` () : unit =
        let graphClient = buildGraphClientAndCleanTestGraph ()

        runBatch graphClient [ GraphTransaction [ AddRelationship relationship ] ]
        countRelationship graphClient |> should equal 1
        runBatch graphClient [  GraphTransaction [ RemoveRelationship relationship ] ]
        countRelationship graphClient |> should equal 0
        