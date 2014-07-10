namespace Eventful.Raven

open Eventful

open Raven.Abstractions.Commands

type BatchWrite = (seq<ProcessAction> * (bool -> Async<unit>))

module BatchOperations =
    let buildPutCommand (writeRequest:DocumentWriteRequest) =
        let cmd = new PutCommandData()
        cmd.Document <- writeRequest.Document.Force()
        cmd.Key <- writeRequest.DocumentKey
        cmd.Etag <- writeRequest.Etag
        cmd.Metadata <- writeRequest.Metadata.Force()
        cmd

    let buildDeleteCommand (deleteRequest:DocumentDeleteRequest) =
        let cmd = new DeleteCommandData()
        cmd.Key <- deleteRequest.DocumentKey
        cmd.Etag <- deleteRequest.Etag
        cmd
        
    let buildCommandFromProcessAction processAction =
        match processAction with
        | Write x -> buildPutCommand x :> ICommandData
        | Delete x -> buildDeleteCommand x :> ICommandData
        
    let writeBatch (documentStore : Raven.Client.IDocumentStore) database (docs:seq<BatchWrite>) = async {
        try 
            let! batchResult = 
                docs
                |> Seq.collect (fst >> Seq.map buildCommandFromProcessAction)
                |> Array.ofSeq
                |> documentStore.AsyncDatabaseCommands.ForDatabase(database).BatchAsync
                |> Async.AwaitTask

            return Some (batchResult, docs)
        with | e -> return None
    }