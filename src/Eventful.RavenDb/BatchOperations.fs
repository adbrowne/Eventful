﻿namespace Eventful.Raven

open Eventful

open Raven.Abstractions.Commands
open Raven.Json.Linq

type BatchWrite = seq<ProcessAction>

module BatchOperations =
    let log = createLogger "Eventful.Raven.BatchOperations"
    let buildPutCommand (documentStore : Raven.Client.IDocumentStore) (writeRequest:DocumentWriteRequest) =
        let cmd = new PutCommandData()
        cmd.Document <- RavenJObject.FromObject(writeRequest.Document, documentStore.Conventions.CreateSerializer())
        cmd.Key <- writeRequest.DocumentKey
        cmd.Etag <- writeRequest.Etag
        cmd.Metadata <- writeRequest.Metadata.Force()
        cmd

    let buildDeleteCommand (deleteRequest:DocumentDeleteRequest) =
        let cmd = new DeleteCommandData()
        cmd.Key <- deleteRequest.DocumentKey
        cmd.Etag <- deleteRequest.Etag
        cmd
        
    let buildCommandFromProcessAction documentStore processAction =
        match processAction with
        | Write (x,request) -> buildPutCommand documentStore x :> ICommandData
        | Delete (x,_) -> buildDeleteCommand x :> ICommandData
        | Custom x -> x
        
    let writeBatch (documentStore : Raven.Client.IDocumentStore) database (docs:seq<BatchWrite>) = async {
        let buildCmd = (buildCommandFromProcessAction documentStore)
        try 
            let! batchResult = 
                docs
                |> Seq.collect (Seq.map buildCmd)
                |> Array.ofSeq
                |> documentStore.AsyncDatabaseCommands.ForDatabase(database).BatchAsync
                |> Async.AwaitTask

            return Choice1Of2 (batchResult, docs)
        with    
            | :? System.AggregateException as e -> 
                log.DebugWithException <| lazy("Write Error", e :> System.Exception)
                log.DebugWithException <| lazy("Write Inner", e.InnerException)
                return Choice2Of2 (e :> System.Exception)
            | e ->
                log.DebugWithException <| lazy("Write Error", e)
                return Choice2Of2 e
    }

    let bulkInsert (documentStore : Raven.Client.IDocumentStore) (inserter : Raven.Client.Document.BulkInsertOperation) (docs:seq<BatchWrite>) = async {
        let buildCmd = (buildCommandFromProcessAction documentStore)
        try 
            for doc in docs do
                for req in doc do
                    match req with
                    | Write (req, _) ->
                        inserter.Store(RavenJObject.FromObject(req.Document, documentStore.Conventions.CreateSerializer()), req.Metadata.Force(), req.DocumentKey)
                    | _ -> ()
        with    
            | :? System.AggregateException as e -> 
                log.DebugWithException <| lazy("Write Error", e :> System.Exception)
                log.DebugWithException <| lazy("Write Inner", e.InnerException)
            | e ->
                log.DebugWithException <| lazy("Write Error", e)
    }