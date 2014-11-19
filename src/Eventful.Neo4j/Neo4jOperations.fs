namespace Eventful.Neo4j

open Eventful

open FSharpx

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

    let matchQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Match(clause) }
    
    let mergeQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Merge(clause) }
    
    let whereQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.Where(clause) }

    let orWhereQ (clause : string) (query : CypherQuery) =
        { query with Query = query.Query.OrWhere(clause) }

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

    let resultsAsyncQ (query : ICypherFluentQuery<'a>) =
        query.ResultsAsync
        |> Async.AwaitTask

    let executeAsyncQ (query : CypherQuery) =
        query.Query.ExecuteWithoutResultsAsync()
        |> Async.AwaitIAsyncResult
        |> Async.Ignore

    // This lets us add another independent query to a single cypher statement.
    // TODO: Use the transaction support http://neo4j.com/docs/stable/rest-api-transactional.html
    //       Neo4jClient doesn't yet support this. CypherNet does, but doesn't support MERGE.
    let chainQ (query : CypherQuery) =
        { query with Query = query.Query.With("1 as dummy") }


    let graphLabel (query : CypherQuery) =
        sprintf "`Graph-%s`" query.GraphName

    [<Literal>]
    let IdPropertyName = "eventful_id"

    // This should be run once for each graph
    let createIdConstraintQ (query : CypherQuery) =
        query
        |> createConstraintQ (graphLabel query) IdPropertyName

    let withNodeSelectorQ (name : string) (nodeId : NodeId) (query : CypherQuery) =
        let query, idParameter = query |> withParamQ IdPropertyName nodeId.Id
        (query, sprintf "(%s:%s:`%s` {%s: %s})" name (graphLabel query) nodeId.Label IdPropertyName idParameter)

    let withNodeWhereClauseQ (name : string) (nodeId : NodeId) (query : CypherQuery) =
        let query, idParameter = query |> withParamQ IdPropertyName nodeId.Id
        (query, sprintf "%s:%s:`%s` AND %s.%s = %s" name (graphLabel query) nodeId.Label name IdPropertyName idParameter)

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
        let query, idParameter = query |> withParamQ IdPropertyName nodeId.Id  // TODO: This parameter is probably already in the query, would be nice to reuse it instead of duplicating it.

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
            query
            |> mergeNodeIdQ "from" from
            |> mergeNodeIdQ "to" to'
            |> mergeQ (sprintf "(from)-[:`%s`]->(to)" relationshipType)

        | RemoveRelationship { From = from; To = to'; Type = relationshipType } ->
            let query, fromSelector = query |> withNodeSelectorQ "from" from
            let query, toSelector = query |> withNodeSelectorQ "to" to'

            query
            |> matchQ (sprintf "(%s)-[r:`%s`]->(%s)" fromSelector relationshipType toSelector)
            |> deleteQ "r"

        | RemoveAllIncomingRelationships (node, relationshipType) ->
            let query, selector = query |> withNodeSelectorQ "" node
            
            query
            |> matchQ (sprintf "()-[r:`%s`]->(%s)" relationshipType selector)
            |> deleteQ "r"

        | UpdateNode (node, data) ->
            query
            |> mergeNodeIdQ "node" node
            |> updateNodeQ "node" node data

    let graphActionSeqToQuery (graphClient : ICypherGraphClient) (graphName : string) actions =
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

    let writeBatch (graphClient : ICypherGraphClient) (graphName : string) (actionBatches : seq<seq<GraphAction>>) =
        async {
            try
                let queries =
                    actionBatches
                    |> Seq.map (graphActionSeqToQuery graphClient graphName)

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
