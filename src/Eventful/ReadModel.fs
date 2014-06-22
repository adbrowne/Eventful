namespace Eventful
    open System

    type IDocumentStore =
        abstract GetDocument<'TDocument> : string -> 'TDocument option
        abstract Store<'TDocument> : string -> 'TDocument -> unit

    type IDocumentWriter =
        abstract EvtType : Type
        abstract DocumentType : Type
        abstract GetDocumentKey : obj -> string
        abstract WriteDocument : IDocumentStore -> obj -> unit

    type DocumentWriterWrapper<'TEvt, 'TDocument> (getKey : 'TEvt -> string, f : 'TDocument -> 'TEvt -> 'TDocument) =
        member m.GetDocumentKey = getKey
        member m.WriteDocument (documentStore : IDocumentStore) (evt : 'TEvt) =
            let key = getKey evt
            let document = 
                match documentStore.GetDocument key with
                | Some document -> document
                | None -> Activator.CreateInstance()

            let updatedDocument = f document evt
            documentStore.Store key updatedDocument
            
        interface IDocumentWriter with
            member m.EvtType = typeof<'TEvt>
            member m.DocumentType = typeof<'TDocument>
            member m.GetDocumentKey evt = m.GetDocumentKey (evt :?> 'TEvt) 
            member m.WriteDocument documentStore (evt : obj) = m.WriteDocument documentStore (evt :?> 'TEvt)

    type ReadModel(documentWriters : IDocumentWriter list, documentStore : IDocumentStore) =
        static let empty documentStore =
            new ReadModel(List.empty, documentStore)

        static member Empty = empty

        member m.Add<'TEvt, 'TDocument> (getKey : 'TEvt -> string) (f : 'TDocument -> 'TEvt -> 'TDocument) =
            let wrappedWriter = new DocumentWriterWrapper<'TEvt, 'TDocument>(getKey, f) :> IDocumentWriter
            new ReadModel(wrappedWriter :: documentWriters, documentStore)

        member m.RunEvent<'TEvt> (evt : 'TEvt) =
            documentWriters
            |> List.filter (fun w -> w.EvtType = evt.GetType())
            |> List.iter (fun w -> w.WriteDocument documentStore evt)