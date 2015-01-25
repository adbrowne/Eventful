namespace Eventful.Tests.Integration

open Xunit
open System
open System.Runtime.Caching
open Eventful.Raven

open FsUnit.Xunit

module RavenOperationTests = 
    let buildDocumentStore () =
        let documentStore = new Raven.Client.Document.DocumentStore()
        documentStore.Url <- "http://localhost:8080"
        documentStore.Initialize() |> ignore
        documentStore

    [<CLIMutable>]
    type MyDoc = {
        Id : string
        Value : string
    }

    let testDb = "tenancy-blue"

    [<Fact>]
    let ``Retrieve non existent doc`` () : unit =
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 
        let cache = new MemoryCache("TestCache")

        let docKey = "DoesNotExist"
        let result = 
            RavenOperations.getDocuments 
                documentStore 
                cache 
                testDb
                (Seq.singleton (docKey, typeof<MyDoc>)) 
            |> Async.RunSynchronously

        result |> Seq.length |> should equal 1

        let (resultDocKey, resultType, resultDoc) = result |> Seq.head

        resultDocKey |> should equal docKey
        resultType |> should equal typeof<MyDoc>
        resultDoc |> should be Null

    [<Fact>]
    let ``Retrieve existing doc not in cache`` () : unit =
        let documentStore = buildDocumentStore() :> Raven.Client.IDocumentStore 
        let cache = new MemoryCache("TestCache")

        let docKey = "MyDocs/" + Guid.NewGuid().ToString()
        let doc = {
            Id = docKey
            Value = "Andrew"
        }

        let saveDoc () =
            use session = documentStore.OpenSession(testDb)
            session.Store doc
            session.SaveChanges()

        saveDoc()

        let result = 
            RavenOperations.getDocuments 
                documentStore 
                cache 
                testDb
                (Seq.singleton (docKey, typeof<MyDoc>)) 
            |> Async.RunSynchronously

        result |> Seq.length |> should equal 1

        let (resultDocKey, resultType, resultDoc) = result |> Seq.head

        let hasValue a b =
            true

        resultDocKey |> should equal docKey
        resultType |> should equal typeof<MyDoc>