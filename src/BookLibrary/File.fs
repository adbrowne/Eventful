namespace BookLibrary

open FSharpx
open Suave
open Suave.Http
open Suave.Types
open Suave.Http.Applicatives
open Suave.Http.Successful
open Suave.Http.RequestErrors
open Suave.Http
open Raven.Client.Connection.Async
open Raven.Json.Linq

module FileWebApi = 
    let saveFile (db : IAsyncDatabaseCommands) (file : HttpUpload) (c : HttpContext) : SuaveTask<HttpContext> = async {
        let fileId = System.Guid.NewGuid()
        let key = sprintf "files/%A" fileId
        let fileData = System.IO.File.ReadAllText(file.Path) // hopefully not too big!
        let body = RavenJObject.Parse(fileData)
        let metadata = new RavenJObject()
        metadata.Add("Raven-Entity-Name", new RavenJValue("Files"))
        do! db.PutAsync(key, Raven.Abstractions.Data.Etag.Empty, body, metadata)
            |> Async.AwaitTask
            |> Async.Ignore

        return! 
            sprintf "{ \"fileId\": \"%A\" }" fileId
            |> UTF8.bytes
            |> accepted
            |> (fun x -> x c)
    }

    let pred f (c : HttpContext) =
        if f c then
            succeed c
        else
            fail

    let hasSingleFile =
        pred (fun c -> c.request.files.Length = 1)

    let hasNoFile =
        pred (fun c -> c.request.files.IsEmpty)

    let moreThanOneFile =
        pred (fun c -> c.request.files.Length > 1)

    let singleFile f : WebPart =
        choose [
            hasSingleFile >>= request(fun r -> f r.files.Head)
            hasNoFile >>= BAD_REQUEST "No file"
            moreThanOneFile >>= BAD_REQUEST "More than one file"
        ]

    let config dbCommands =
        url "/api/files" 
        >>= POST 
        >>= singleFile (saveFile dbCommands)