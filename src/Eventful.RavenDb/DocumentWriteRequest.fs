namespace Eventful.Raven

open System
open Raven.Json.Linq
open Raven.Abstractions.Data
open System.Threading.Tasks

type ProjectedDocument<'TDocument> = ('TDocument * Raven.Json.Linq.RavenJObject * Raven.Abstractions.Data.Etag)

[<RequireQualifiedAccess>]
type AccessMode =
    | Read       //< Read-only access.
    | Update     //< Read/write access.

type GetDocRequest =
    {
        DocumentKey : string
        DocumentType : Type
        AccessMode : AccessMode
    }

type IDocumentFetcher =
    abstract member GetDocument<'TDocument> : AccessMode -> string -> Task<ProjectedDocument<'TDocument> option> 
    abstract member GetDocuments : GetDocRequest seq -> Task<(string * System.Type * Option<obj * RavenJObject * Etag>) seq>
    abstract member GetEmptyMetadata<'TDocument> : unit -> RavenJObject

type DocumentWriteRequest = {
    DocumentKey : string
    Document : obj
    Metadata : Lazy<Raven.Json.Linq.RavenJObject>
    Etag : Raven.Abstractions.Data.Etag
}

type DocumentDeleteRequest = {
    DocumentKey : string
    Etag : Raven.Abstractions.Data.Etag
}

type ProcessAction = 
| Write of DocumentWriteRequest * Guid
| Delete of DocumentDeleteRequest * Guid
| Custom of Raven.Abstractions.Commands.ICommandData