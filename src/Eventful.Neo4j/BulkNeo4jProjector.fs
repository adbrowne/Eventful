namespace Eventful.Neo4j

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Eventful

open Neo4jClient

module BulkNeo4jProjector =
    let create
        (
            graphClient : ICypherGraphClient,
            graphName : string,
            matchingKeys : 'TMessage -> seq<NodeId>,
            processMessages : (NodeId * seq<'TMessage>) -> Func<Task<seq<GraphAction>>>,  /// returns a func so the task does not start immediately when using Async.StartAsTask
            maxEventQueueSize : int,
            eventWorkers : int,
            workTimeout : TimeSpan option,
            onEventComplete : 'TMessage -> Async<unit>,
            cancellationToken : CancellationToken,
            writeQueue : Neo4jWriteQueue
        ) =

        let eventProcessor =
            { MatchingKeys = matchingKeys
              Process = (fun (key, events) ->
                (fun () ->
                    async { 
                        let! actions = processMessages(key, events).Invoke() |> Async.AwaitTask
                        return! writeQueue.Work graphName actions
                    }
                    |> Async.StartAsTask)
                |> (fun x -> Func<_> x) ) }

        let positionNodeId =
            { Label = Neo4jConstants.PositionNodeLabel
              Id = Neo4jConstants.PositionNodeId }

        let getPersistedPosition =
            Operations.getNode graphClient graphName positionNodeId  // TODO: Use read queue

        let writeUpdatedPosition position = async {
            let writeRequests =
                UpdateNode (positionNodeId, position)
                |> Seq.singleton

            let! writeResult = writeQueue.Work graphName writeRequests

            return writeResult |> (function Choice1Of2 _ -> true | _ -> false)
        }

        BulkProjector<_, 'TMessage>(
            eventProcessor,
            graphName,
            maxEventQueueSize,
            eventWorkers,
            onEventComplete,
            getPersistedPosition,
            writeUpdatedPosition,
            cancellationToken,
            workTimeout)
