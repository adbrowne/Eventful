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
            graphName : string,
            projectors : IProjector<'TMessage, unit, GraphAction> seq,
            cancellationToken : CancellationToken,
            onEventComplete : 'TMessage -> Async<unit>,
            graphClient : ICypherGraphClient,
            writeQueue : Neo4jWriteQueue,
            maxEventQueueSize : int,
            eventWorkers : int,
            workTimeout : TimeSpan option
        ) =
        let executor actions =
            writeQueue.Work graphName actions

        let positionNodeId = Neo4jConstants.PositionNodeId

        let getPersistedPosition =
            Operations.getNode graphClient graphName positionNodeId  // TODO: Use read queue

        let writeUpdatedPosition position = async {
            let writeRequests =
                UpdateNode (positionNodeId, position)
                |> Seq.singleton

            let! writeResult = writeQueue.Work graphName writeRequests

            return writeResult |> (function Choice1Of2 _ -> true | _ -> false)
        }

 
        let projectors =
            BulkProjector.projectorsWithContext projectors ()
            |> Seq.toArray

        BulkProjector<_, _>(
            "Neo4j-" + graphName,
            projectors,
            executor,
            cancellationToken,
            onEventComplete,
            getPersistedPosition,
            writeUpdatedPosition,
            maxEventQueueSize,
            eventWorkers,
            workTimeout)
