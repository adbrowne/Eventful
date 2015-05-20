namespace Eventful.Raven

type DocumentFetcher 
    (
        documentStore:Raven.Client.IDocumentStore,
        databaseName: string,
        readQueue : RavenReadQueue
    ) =
    
    interface IDocumentFetcher with
        member x.GetDocument<'TDocument> accessMode key = 
            async {
                let request =
                    {
                        DocumentKey = key
                        DocumentType = typeof<'TDocument>
                        AccessMode = accessMode
                    }
                    |> Seq.singleton

                let! result = readQueue.Work databaseName request
                let (key, t, result) = Seq.head result

                match result with
                | Some (doc, metadata, etag) -> 
                    return (Some (doc :?> 'TDocument, metadata, etag))
                | None -> 
                    return None 
            } |> Async.StartAsTask

        member x.GetDocuments request = 
            async {
                return! readQueue.Work databaseName request 
            } |> Async.StartAsTask

        member x.GetEmptyMetadata<'TDocument> () =
            RavenOperations.emptyMetadataForType documentStore typeof<'TDocument>