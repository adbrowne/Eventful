namespace BookLibrary

open System
open System.IO
open Eventful
open EventStore.ClientAPI
open Eventful.EventStore
open Eventful.Raven
open System.Threading.Tasks
open Suave
open Suave.Http.Successful
open Suave.Http
open Suave.Http.Applicatives
open Suave.Web

type EventCountDoc = {
    Count : int
}

type TopShelfService () =
    let log = createLogger "EmergencyRoom.EmergencyRoomTopShelfService"

    let mutable client : Client option = None
    let mutable eventStoreSystem : BookLibraryEventStoreSystem option = None

    let matchingKeys (message : EventStoreMessage) =
        let methodWithoutGeneric = Book.documentBuilder.GetType().GetMethod("GetKeysFromEvent", Reflection.BindingFlags.Public ||| Reflection.BindingFlags.Instance)
        let genericMethod = methodWithoutGeneric.MakeGenericMethod([|message.Event.GetType()|])
        let keys = genericMethod.Invoke(Book.documentBuilder, [|message.Event; message.EventContext|]) :?> string list

        keys |> Seq.ofList

    let runMessage (docKey : string) (document : Book.BookDocument) (message : EventStoreMessage) =
        let methodWithoutGeneric = Book.documentBuilder.GetType().GetMethod("ApplyEvent", Reflection.BindingFlags.Public ||| Reflection.BindingFlags.Instance)
        let genericMethod = methodWithoutGeneric.MakeGenericMethod([|message.Event.GetType()|])
        genericMethod.Invoke(Book.documentBuilder, [|docKey; document; message.Event; message.EventContext|]) :?> Book.BookDocument

    let processVisitEvent documentStore (documentFetcher:IDocumentFetcher) (docKey:string) (messages : seq<EventStoreMessage>) = async {
        let! doc = documentFetcher.GetDocument<Book.BookDocument>(docKey) |> Async.AwaitTask
        let bookId : BookId = 
            messages
            |> Seq.head
            |> (fun x -> MagicMapper.magicId x.Event)
        let (doc, metadata, etag) = 
            match doc with
            | Some x -> x
            | None -> 
                let doc = Book.BookDocument.NewDoc bookId
                let metadata = RavenOperations.emptyMetadata<Book.BookDocument> documentStore
                let etag = Raven.Abstractions.Data.Etag.Empty
                (doc, metadata, etag)

        let doc = 
            messages
            |> Seq.fold (runMessage docKey) doc

        return seq {
            let write = {
               DocumentKey = docKey
               Document = doc
               Metadata = lazy(metadata) 
               Etag = etag
            }
            yield Write (write, Guid.NewGuid())
        }
    }

    member x.Start () =

        log.Debug <| lazy "Starting App"
        async {
            let! connection = ApplicationConfig.getConnection()
            let c = new Client(connection)

            let documentStore = ApplicationConfig.buildDocumentStore()

            let system : BookLibraryEventStoreSystem = ApplicationConfig.buildEventStoreSystem documentStore c
            system.Start() |> Async.StartAsTask |> ignore

            let bookLibrarySystem = new BookLibrarySystem(system)

            // start web
            choose 
                [ BooksWebApi.config bookLibrarySystem
                  (OK "Hello World!") ]
            |> web_server default_config 

            let projector = {
                MatchingKeys = matchingKeys
                ProcessEvents = processVisitEvent documentStore
            }

            let cache = new System.Runtime.Caching.MemoryCache("myCache")

            let writeQueue = new RavenWriteQueue(documentStore, 100, 10000, 10, Async.DefaultCancellationToken, cache)
            let readQueue = new RavenReadQueue(documentStore, 100, 1000, 10, Async.DefaultCancellationToken, cache)

            let bulkRavenProjector =    
                BulkRavenProjector.create
                    (
                        ApplicationConfig.dbName,
                        Seq.singleton projector,
                        Async.DefaultCancellationToken,  
                        (fun _ -> async { () }),
                        documentStore,
                        writeQueue,
                        readQueue,
                        100000, 
                        1000, 
                        Some (TimeSpan.FromSeconds(60.0))
                    )
            bulkRavenProjector.StartWork ()
            bulkRavenProjector.StartPersistingPosition ()

            let lastPosition = 
                bulkRavenProjector.LastComplete() 
                |> Async.RunSynchronously 
                |> Option.map (fun eventPosition -> new EventStore.ClientAPI.Position(eventPosition.Commit, eventPosition.Prepare))

            let handle id (re : EventStore.ClientAPI.ResolvedEvent) =
                match system.EventStoreTypeToClassMap.ContainsKey re.Event.EventType with
                | true ->
                    let eventClass = system.EventStoreTypeToClassMap.Item re.Event.EventType
                    let evtObj = Serialization.esSerializer.DeserializeObj re.Event.Data eventClass
                    let metadata = Serialization.esSerializer.DeserializeObj re.Event.Metadata typeof<BookLibraryEventMetadata> :?> BookLibraryEventMetadata

                    let eventStoreMessage : EventStoreMessage = {
                        EventContext = metadata
                        Id = re.Event.EventId
                        Event = evtObj
                        StreamIndex = re.Event.EventNumber
                        EventPosition = { Commit = re.OriginalPosition.Value.CommitPosition; Prepare = re.OriginalPosition.Value.PreparePosition }
                        StreamName = re.Event.EventStreamId
                    }

                    bulkRavenProjector.Enqueue (eventStoreMessage)
                | false -> async { () }

            let onLive _ = ()

            c.subscribe lastPosition handle onLive |> ignore

            client <- Some c
            eventStoreSystem <- Some system
        } |> Async.StartAsTask

    member x.Stop () =
        log.Debug <| lazy "App Stopping"