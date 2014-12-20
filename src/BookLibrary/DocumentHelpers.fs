namespace BookLibrary

module DocumentHelpers = 
    let getDeliveryDocument (openSession : unit -> Raven.Client.IAsyncDocumentSession) (fileId : FileId) = async {
        use session = openSession () 
        let key = sprintf "files/%A" fileId.Id
        let! deliveryJObject = session.LoadAsync<Raven.Json.Linq.RavenJObject>(key) |> Async.AwaitTask

        return
            deliveryJObject.ToString()
            |> System.Text.Encoding.UTF8.GetBytes
            |> (fun x -> Serialization.deserializeObj x typeof<DeliveryDocument>)
            :?> DeliveryDocument }