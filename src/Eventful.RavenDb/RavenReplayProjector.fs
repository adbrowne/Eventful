namespace Eventful.Raven

open System
open System.Threading
open System.Runtime.Caching

open Eventful
open Metrics

open FSharpx

open Raven.Client
open Raven.Abstractions.Data
open Raven.Json.Linq
open FSharp.Collections.ParallelSeq

type RavenReplayProjector<'TMessage when 'TMessage :> IBulkMessage> 
    (
        documentStore:Raven.Client.IDocumentStore, 
        projectors : IProjector<'TMessage, IDocumentFetcher, ProcessAction> seq,
        databaseName: string
    ) =

    let log = createLogger "Eventful.Raven.RavenReplayProjector"
    let numWorkers = 10

    

    let fetcher = {
        new IDocumentFetcher with
            member x.GetDocument<'TDocument> accessMode key : Tasks.Task<ProjectedDocument<'TDocument> option> = 
                async {
                    return None 
                } |> Async.StartAsTask
            member x.GetDocuments request = 
                async {
                    return
                        request
                        |> Seq.map(fun { DocumentKey = a; DocumentType = b } -> (a,b,None))
                } |> Async.StartAsTask

            member x.GetEmptyMetadata<'TDocument> () =
                RavenOperations.emptyMetadataForType documentStore typeof<'TDocument>
    }

    let mutable messages : 'TMessage list = List.Empty
    let mutable inserts : ProcessAction seq = Seq.empty

    let projectors =
        BulkProjector.projectorsWithContext projectors fetcher
        |> Seq.toArray

    let documentsWithKeys msg =
        BulkProjector.allMatchingKeys projectors msg

    let queuedMessagesCounter = Metric.Counter(databaseName + " RavenReplayProjector documents awaiting projection", Unit.Items)

    let accumulateItems s ((key, projectorIndex), items) = async {
        let events = items |> Seq.map fst
        let projector = projectors.[projectorIndex]

        try
            let! writeRequests, _ = projector.ProcessEvents key events
            queuedMessagesCounter.Decrement()
            return writeRequests :: s
        with
        | ex ->
            log.ErrorWithException <| lazy ("Exception during ProcessEvents", ex)
            return s
    }

    let printReport v =
        log.Debug <| lazy v
    
    member x.DatabaseName: string = databaseName

    member x.Enqueue (message : 'TMessage) =
        messages <- message::messages

    member x.ProcessQueuedItems() =
        log.Debug <| lazy(sprintf "ProcessQueuedItems: %A. Count: %A" databaseName messages.Length)

        inserts <-
            messages
            // reverse messages so they run in order
            |> List.rev
            |> PSeq.ordered
            |> PSeq.map documentsWithKeys
            // route events to documents
            |> PSeq.collect id
            // group events into groups by document
            |> PSeq.groupBy snd
            // group items into numWorkers batches
            |> PSeq.mapi(fun i x ->
                queuedMessagesCounter.Increment()
                (i % numWorkers,x))
            |> PSeq.groupBy fst
            // map to async tasks
            |> PSeq.map (fun (_, workItems) -> async {
                let docs = 
                    workItems
                    |> Seq.map snd
                let! reversedItems = Async.foldM accumulateItems [] docs
                let result = List.rev reversedItems |> Seq.concat
                return result
            })
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Array.toSeq
            |> Seq.collect id

    member x.InsertDocuments() =
        log.Debug <| lazy(sprintf "%s: Starting document insert" databaseName)

        let duplicateKeys, _ =
            inserts
            |> Seq.fold
                (fun ((duplicateKeys, seenKeys) as state) insert ->
                    match insert with
                    | Write (writeRequest, _) ->
                        let lowerKey = writeRequest.DocumentKey.ToLowerInvariant()
                        if seenKeys |> Set.contains lowerKey then
                            (writeRequest.DocumentKey :: duplicateKeys, seenKeys)
                        else
                            (duplicateKeys, seenKeys |> Set.add lowerKey)
                    | _ -> state)
                ([], Set.empty)

        if not (List.isEmpty duplicateKeys) then
            failwith ("Found duplicate keys in insert: " + (String.concat ", " duplicateKeys))

        use bulkInsert = documentStore.BulkInsert(databaseName)
        bulkInsert.add_Report(fun s -> printReport s)
        for insert in inserts do
            match insert with
            | Write (writeRequest, _) ->
                let doc = RavenJObject.FromObject(writeRequest.Document, documentStore.Conventions.CreateSerializer())
                bulkInsert.Store(doc, writeRequest.Metadata.Force(), writeRequest.DocumentKey)
            | Delete _ -> () // don't do anything for delete  // TODO: We should have already dealt with these and thrown away any preceeding document
            | Custom _ -> failwith "Cannot support custom operations"

        messages <- []
        log.Debug <| lazy(sprintf "%s: Finalizing document insert (waiting for success notification from BulkInsert.Dispose)" databaseName)

    member x.WritePosition (position : EventPosition) =
        let key = RavenConstants.PositionDocumentKey
        let doc = position
        let metadata = RavenOperations.emptyMetadata<EventPosition> documentStore
        RavenOperations.writeDoc documentStore databaseName key doc metadata
        |> Async.RunSynchronously