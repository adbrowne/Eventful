﻿namespace Eventful.Neo4j

open System
open System.Linq.Expressions
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Linq.RuntimeHelpers
open Newtonsoft.Json

open Eventful

open FSharpx
open FSharp.Data

open Neo4jClient
open Neo4jClient.Cypher

// TODO: Deal with backticks in label names
// TODO: Cache

module Operations =
    let log = createLogger "Eventful.Neo4j.Operations"

    type CypherQuery =
        { Query : ICypherFluentQuery
          GraphName : string
          NextParameterIndex : int  // Used to make sure parameter names are unique
        }

    let beginQ (graphClient : ICypherGraphClient) graphName =
        { Query = graphClient.Cypher
          GraphName = graphName
          NextParameterIndex = 0 }

    let createConstraintQ (label : string) (propertyName : string) (query : CypherQuery) =
        { query with Query = query.Query.CreateUniqueConstraint("node:" + label, "node." + propertyName) }

    let createQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Create(clause) }

    let matchQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Match(clause) }
    
    let mergeQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Merge(clause) }
    
    let whereQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Where(clause) }

    let orWhereQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.OrWhere(clause) }

    let withQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.With(clause) }

    let optionalMatchQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.OptionalMatch(clause) }

    let returnExprQ (expr : Expr<'a>) (query : CypherQuery) =
        let lambdaExpression =
            Expression.Lambda(LeafExpressionConverter.QuotationToExpression expr, Seq.empty)
            :?> Expression<Func<'a>>

        query.Query.Return(lambdaExpression)

    let withParamQ name value (query : CypherQuery) =
        let parameterName = sprintf "%s_%i" name query.NextParameterIndex
        let query' =
            { query with
                Query = query.Query.WithParam(parameterName, value)
                NextParameterIndex = query.NextParameterIndex + 1 }

        (query', sprintf "{%s}" parameterName)

    let setQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Set(clause) }

    let deleteQ (name : string) (query : CypherQuery) =
        { query with Query = query.Query.Delete(name) }

    let returnQ (name : string) (query : CypherQuery) =
        query.Query.Return(name)

    let resultsQ (query : ICypherFluentQuery<'a>) =
        query.Results

    let resultsAsyncQ (query : ICypherFluentQuery<'a>) =
        query.ResultsAsync
        |> Async.AwaitTask

    let executeQ (query : CypherQuery) =
        query.Query.ExecuteWithoutResults()

    let executeAsyncQ (query : CypherQuery) =
        query.Query.ExecuteWithoutResultsAsync()
        |> voidTaskAsAsync

    // This lets us add another independent query to a single cypher statement.
    // TODO: Use the transaction support http://neo4j.com/docs/stable/rest-api-transactional.html
    //       Neo4jClient doesn't yet support this. CypherNet does, but doesn't support MERGE.
    let chainQ (query : CypherQuery) =
        { query with Query = query.Query.With("count(*) as dummy") }


    let graphLabel (query : CypherQuery) =
        sprintf "`Graph-%s`" query.GraphName

    [<Literal>]
    let IdPropertyName = "eventful_id"

    // This should be run once for each graph
    let createIdConstraintQ (query : CypherQuery) =
        query
        |> createConstraintQ (graphLabel query) IdPropertyName

    let withNodeSelectorQ (name : string) (nodeId : NodeId) (query : CypherQuery) =
        let query, idParameter = query |> withParamQ IdPropertyName nodeId
        (query, sprintf "(%s:%s {%s: %s})" name (graphLabel query) IdPropertyName idParameter)

    let withNodeWhereClauseQ (name : string) (nodeId : NodeId) (query : CypherQuery) =
        let query, idParameter = query |> withParamQ IdPropertyName nodeId
        (query, sprintf "%s:%s AND %s.%s = %s" name (graphLabel query) name IdPropertyName idParameter)

    let matchOrMergeNodeIdQ matchOrMerge (name : string) (nodeId : NodeId) (query : CypherQuery) =
        let query, selector = query |> withNodeSelectorQ name nodeId
        query |> matchOrMerge selector

    let matchNodeIdQ = matchOrMergeNodeIdQ matchQ
    let mergeNodeIdQ = matchOrMergeNodeIdQ mergeQ

    let whereNodeIdQ addWhere (name : string) (nodeId : NodeId) (query : CypherQuery) =
        let query, clause = query |> withNodeWhereClauseQ name nodeId
        query |> addWhere clause
    
    let updateNodeQ (name : string) (nodeId : NodeId) (data : obj) (query : CypherQuery) =
        let query, dataParameter = query |> withParamQ "data" data
        let query, idParameter = query |> withParamQ IdPropertyName nodeId // TODO: This parameter is probably already in the query, would be nice to reuse it instead of duplicating it.

        query
        |> setQ (sprintf "%s = %s, %s.%s = %s" name dataParameter name IdPropertyName idParameter)  // Because we're replacing all the parameters, we have to make sure to set the id property again.


    let getNode (graphClient : ICypherGraphClient) (graphName : string) (nodeId : NodeId) =
        async {
            let! results =
                beginQ graphClient graphName
                |> matchNodeIdQ "node" nodeId
                |> returnQ "node"
                |> resultsAsyncQ
                
            let noneOneMany =
                results
                |> Seq.truncate 2
                |> Seq.toList

            return
                match noneOneMany with
                | [] -> None
                | [ result ] -> Some result
                | _ -> failwith (sprintf "Multiple nodes found in graph %s that match id %A" graphName nodeId)
        }

    // All results currently have to be of the same type.
    // Neo4jClient doesn't seem to support heterogeneous result sets but need to confirm to be sure.
    let getNodes (graphClient : ICypherGraphClient) (graphName : string) (nodeIds : NodeId seq) =
        if Seq.isEmpty nodeIds then Async.returnM Seq.empty else

        let nodeName = "node"

        let folder (query, isFirst) nodeId =
            let whereMode = if isFirst then whereQ else orWhereQ
            (query |> whereNodeIdQ whereMode nodeName nodeId, false)

        let initialQuery =
            beginQ graphClient graphName
            |> matchQ nodeName

        nodeIds
        |> Seq.fold folder (initialQuery, true)
        |> fst
        |> returnQ nodeName
        |> resultsAsyncQ

    let appendGraphActionToQuery action (query : CypherQuery) =
        match action with
        | AddRelationship { From = from; To = to'; Type = relationshipType } ->
            let relationship = sprintf "(to)<-[:`%s`]-(from)" relationshipType

            query
            |> mergeNodeIdQ "from" from
            |> mergeNodeIdQ "to" to'
            |> withQ "from, to"
            |> whereQ (sprintf "shortestPath(%s) IS NULL" relationship)
            |> createQ relationship

        | RemoveRelationship { From = from; To = to'; Type = relationshipType } ->
            let query, fromSelector = query |> withNodeSelectorQ "from" from
            let query, toSelector = query |> withNodeSelectorQ "to" to'

            query
            |> optionalMatchQ (sprintf "%s-[r:`%s`]->%s" fromSelector relationshipType toSelector)
            |> deleteQ "r"

        | RemoveAllIncomingRelationships (node, relationshipType) ->
            let query, selector = query |> withNodeSelectorQ "" node
            
            query
            |> optionalMatchQ (sprintf "()-[r:`%s`]->%s" relationshipType selector)
            |> deleteQ "r"

        | AddLabels (node, labels) ->
            let setLabels =
                Seq.append (Seq.singleton "node") labels
                |> String.concat ":"
            
            query
            |> mergeNodeIdQ "node" node
            |> setQ setLabels

        | UpdateNode (node, data) ->
            query
            |> mergeNodeIdQ "node" node
            |> updateNodeQ "node" node data

        | IncrementVersion node ->
            query
            |> mergeNodeIdQ "node" node
            |> setQ "node.version = coalesce(node.version + 1, 0)"

    let graphTransactionToQuery (graphClient : ICypherGraphClient) (graphName : string) (GraphTransaction actions) =
        let folder (query, isFirst) action =
            let chainIfNotFirst = if isFirst then id else chainQ

            let query =
                query
                |> chainIfNotFirst
                |> appendGraphActionToQuery action

            (query, false)

        actions
        |> Seq.fold folder (beginQ graphClient graphName, true)
        |> fst

    let writeBatch (graphClient : ICypherGraphClient) (graphName : string) (actionBatches : seq<seq<GraphTransaction>>) =
        async {
            try
                let queries =
                    actionBatches
                    |> Seq.concat
                    |> Seq.map (graphTransactionToQuery graphClient graphName)

                for query in queries do
                    do! executeAsyncQ query

                return Choice1Of2 ()
            with    
                | :? System.AggregateException as e -> 
                    log.DebugWithException <| lazy("Write Error", e :> System.Exception)
                    log.DebugWithException <| lazy("Write Inner", e.InnerException)
                    return Choice2Of2 (e :> System.Exception)
                | e ->
                    log.DebugWithException <| lazy("Write Error", e)
                    return Choice2Of2 e
        }

    type ResponseJson = JsonProvider<"""{"results":[{"columns":[],"data":[]}],"errors":[{"code":"error code","message":"error message"}]}""">

    let emptyStatements = """ { "statements": [] } """

    let post uri body =
        Http.Request(uri, headers = [ HttpRequestHeaders.ContentType HttpContentTypes.Json ], body = TextRequest body)

    let parseResponse (response : HttpResponse) =
        match response.Body with
        | Text text -> ResponseJson.Parse text
        | _ -> failwith "got binary response"

    let checkForErrors response =
        let jsonResponse = parseResponse response
        if (not << Seq.isEmpty) jsonResponse.Errors then
            failwith (sprintf "Got errors: %A" jsonResponse.Errors)

    let beginTransaction neo4jEndpoint =
        let transactionEndpoint = neo4jEndpoint + "/transaction"
        let response = post transactionEndpoint emptyStatements
        response.Headers |> Map.find "Location"

    let commitTransaction location =
        log.Debug <| lazy "committing Neo4J transaction."
        post (location + "/commit") emptyStatements |> checkForErrors

    let addStatementsToTransaction location (statements : CypherQuery seq) =
        let statementsJson =
            statements
            |> Seq.map (fun query ->
                let statement = query.Query.Query.QueryText |> JsonConvert.SerializeObject
                let parameters = query.Query.Query.QueryParameters |> JsonConvert.SerializeObject
                sprintf """{ "statement": %s, "parameters": %s }""" statement parameters)
            |> String.concat ","

        let body = """ { "statements": [ """ + statementsJson + " ] }"

        post location body |> checkForErrors

    type ITransactionBatcher =
        abstract Add :  string -> GraphTransaction seq -> Async<Choice<unit, exn>>
        abstract QueueLength : int
        abstract Finish : unit -> unit

    type TransactionBatcherMessage<'a> =
        | Item of 'a
        | Finish of AsyncReplyChannel<unit>

    let createTransactionBatcher neo4jEndpoint (graphClient : ICypherGraphClient) =
        let queriesPerBatch = 1000
        let batchesPerTransaction = 50

        let agent = Control.MailboxProcessor.Start(fun inbox ->
            let emptyState = (0, List.empty, 0, None)

            let rec loop (queryCount, batch, batchCount, transaction) =
                async {
                    let! message = inbox.Receive()

                    let newStateOrCompleteBatch =
                        match message with
                        | Item item ->
                            let newBatch = item :: batch
                            let newQueryCount = queryCount + 1
                            if newQueryCount < queriesPerBatch then
                                Choice1Of2 (newQueryCount, newBatch, batchCount, transaction)
                            else
                                Choice2Of2 (newBatch, None)
                        | Finish replyChannel ->
                            // No more queries
                            Choice2Of2 (batch, Some replyChannel)

                    match newStateOrCompleteBatch with
                    | Choice1Of2 newState ->
                        return! loop newState
                    | Choice2Of2 (reversedBatch, finalBatchReplyChannel) ->
                        let transaction = transaction |> Option.getOrElseLazy (lazy beginTransaction neo4jEndpoint)

                        List.rev reversedBatch
                        |> Seq.map (fun (graphName, actions) -> graphTransactionToQuery graphClient graphName actions)
                        |> addStatementsToTransaction transaction

                        match finalBatchReplyChannel with
                        | Some replyChannel ->
                            commitTransaction transaction
                            replyChannel.Reply()
                            return ()
                        | None ->
                            let newBatchCount = batchCount + 1
                            if newBatchCount < batchesPerTransaction then
                                return! loop (0, List.empty, newBatchCount, Some transaction)
                            else
                                commitTransaction transaction
                                return! loop emptyState
                }

            loop emptyState)

        agent.Error.Add (fun exn -> log.ErrorWithException <| lazy ("Neo4j transaction batcher failed", exn))

        { new ITransactionBatcher with
            member x.Add graphName actions =
                async {
                    for action in actions do
                        agent.Post (Item (graphName, action))

                    return Choice1Of2 ()
                }

            member x.QueueLength = agent.CurrentQueueLength

            member x.Finish () = agent.PostAndReply Finish }
