namespace Eventful.Raven

open Eventful

open Raven.Abstractions.Commands
open Raven.Json.Linq

type BatchWrite = (seq<ProcessAction> * (bool -> Async<unit>))

module BatchOperations =
    let log = Common.Logging.LogManager.GetLogger(typeof<BatchWrite>)
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
        | Write x -> buildPutCommand documentStore x :> ICommandData
        | Delete x -> buildDeleteCommand x :> ICommandData
        
    let writeBatch (documentStore : Raven.Client.IDocumentStore) database (docs:seq<BatchWrite>) = async {
        let buildCmd = (buildCommandFromProcessAction documentStore)
        try 
            let! batchResult = 
                docs
                |> Seq.collect (fst >> Seq.map buildCmd)
                |> Array.ofSeq
                |> documentStore.AsyncDatabaseCommands.ForDatabase(database).BatchAsync
                |> Async.AwaitTask

            return Some (batchResult, docs)
        with | e -> 
            log.Error(sprintf "Write Error: %A" e.Message)
            return None
    }