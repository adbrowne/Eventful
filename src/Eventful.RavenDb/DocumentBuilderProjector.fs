namespace Eventful.Raven

open System
open Eventful

type MessageOperations<'TMessage, 'TBaseEvent, 'TMetadata> = {
    GetEvent : 'TMessage -> 'TBaseEvent
    GetMetadata : 'TMessage -> 'TMetadata 
}

module DocumentBuilderProjector =

    let matchingKeys (documentBuilder : DocumentBuilder<'TDocumentKey,'TDocument, 'TMetadata>) (projectorMessage : MessageOperations<'TMessage, 'TBaseEvent, 'TMetadata>) =
        let methodWithoutGeneric = documentBuilder.GetType().GetMethod("GetKeysFromEvent", Reflection.BindingFlags.Public ||| Reflection.BindingFlags.Instance)

        fun (message : 'TMessage) -> 
            let event = projectorMessage.GetEvent message
            let metadata = projectorMessage.GetMetadata message
            let genericMethod = methodWithoutGeneric.MakeGenericMethod([|event.GetType()|])
            let keys = genericMethod.Invoke(documentBuilder, [|event; metadata|]) :?> 'TDocumentKey list

            keys 
            |> Seq.ofList

    let runMessage (documentBuilder : DocumentBuilder<'TDocumentKey,'TDocument, 'TMetadata>) (docKey : string) (document : 'TDocument) (event, metadata) =
        let methodWithoutGeneric = documentBuilder.GetType().GetMethod("ApplyEvent", Reflection.BindingFlags.Public ||| Reflection.BindingFlags.Instance)
        let genericMethod = methodWithoutGeneric.MakeGenericMethod([|event.GetType()|])
        genericMethod.Invoke(documentBuilder, [|docKey; document; event; metadata|]) :?> 'TDocument

    let processEvents (documentBuilder : DocumentBuilder<'TDocumentKey,'TDocument, 'TMetadata>) documentStore (projectorMessage : MessageOperations<'TMessage, 'TBaseEvent, 'TMetadata>) (documentFetcher:IDocumentFetcher) (key:IComparable) (messages : seq<'TMessage>) = async {
        let key = (key :?> 'TDocumentKey) // presume we get back out what we put in
        let docKey = documentBuilder.GetDocumentKey key
        let! doc = documentFetcher.GetDocument<'TDocument> AccessMode.Update docKey |> Async.AwaitTask   // Using AccessMode.Update here because can't guarantee that 'TDocument is immutable
        let (doc, metadata, etag) = 
            match doc with
            | Some x -> x
            | None -> 
                let doc = documentBuilder.NewDocument key
                let metadata = RavenOperations.emptyMetadata<'TDocument> documentStore
                let etag = Raven.Abstractions.Data.Etag.Empty
                (doc, metadata, etag)

        let doc = 
            messages
            |> Seq.map (fun m -> (projectorMessage.GetEvent m, projectorMessage.GetMetadata m))
            |> Seq.fold (runMessage documentBuilder docKey) doc

        return (seq {
            let write = {
               DocumentKey = docKey
               Document = doc
               Metadata = lazy(metadata) 
               Etag = etag
            }
            yield Write (write, Guid.NewGuid())
        }, async.Zero())

    }

    let buildProjector documentStore (builder : DocumentBuilder<'TDocumentKey,'T, 'TMetadata>) getEvent getMetadata =
        
        let messageOperations = {
            GetEvent = getEvent
            GetMetadata = getMetadata
        }
        
        let projector : Projector<'TDocumentKey, 'TMessage, IDocumentFetcher, ProcessAction>= {
            MatchingKeys = matchingKeys builder messageOperations
            ProcessEvents = processEvents builder documentStore messageOperations
        } 
        projector
