namespace Eventful.Raven

open Eventful

open Raven.Abstractions.Commands

type BatchWrite = (seq<DocumentWriteRequest> * (bool -> Async<unit>))

module BatchOperations =
    let buildPutCommand (writeRequest:DocumentWriteRequest) =
        let cmd = new PutCommandData()
        cmd.Document <- writeRequest.Document.Force()
        cmd.Key <- writeRequest.DocumentKey
        cmd.Etag <- writeRequest.Etag
        cmd.Metadata <- writeRequest.Metadata.Force()
        cmd
        
    let writeBatch (documentStore : Raven.Client.IDocumentStore) database (docs:seq<BatchWrite>) = async {
        try 
            let! batchResult = 
                docs
                |> Seq.collect (fst >> Seq.map buildPutCommand)
                |> Seq.cast<ICommandData>
                |> Array.ofSeq
                |> documentStore.AsyncDatabaseCommands.ForDatabase(database).BatchAsync
                |> Async.AwaitTask

            return Some (batchResult, docs)
        with | e -> return None
    }