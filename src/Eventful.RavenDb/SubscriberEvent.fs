namespace Eventful.Raven

type ProjectedDocument<'TDocument> = ('TDocument * Raven.Json.Linq.RavenJObject * Raven.Abstractions.Data.Etag)

type IDocumentFetcher =
    abstract member GetDocument<'TDocument> : string -> Async<ProjectedDocument<'TDocument> option> 

type DocumentWriteRequest = {
    DocumentKey : string
    Document : Lazy<Raven.Json.Linq.RavenJObject>
    Metadata : Lazy<Raven.Json.Linq.RavenJObject>
    Etag : Raven.Abstractions.Data.Etag
}

type SubscriberEvent<'TContext> = {
    Event : obj
    Context : 'TContext
    StreamId : string
    EventNumber: int
}