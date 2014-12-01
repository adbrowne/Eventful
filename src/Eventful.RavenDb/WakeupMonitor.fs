namespace Eventful.Raven

open System
open Eventful
open FSharp.Control
open System.Threading

type WakeupMonitorEvents =
| TimerTick
| IndexChanged

type WakeupMonitor<'TAggregateType>
    (
       documentStore : Raven.Client.Document.DocumentStore,
       database : string,
       serializer: ISerializer,
       onStreamWakeup : string -> 'TAggregateType -> DateTime -> unit
    ) =

    let dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(database)

    let getAggregateType (token : Raven.Json.Linq.RavenJToken) = 
        if typeof<'TAggregateType> = typeof<string> then
            token.Value<string>() :> obj :?> 'TAggregateType
        else
            serializer.DeserializeObj (System.Text.Encoding.UTF8.GetBytes(token.ToString())) typeof<'TAggregateType> :?> 'TAggregateType
        
    let getWakeups time = 
        let rec loop start = asyncSeq {
            let indexQuery = new Raven.Abstractions.Data.IndexQuery()
            indexQuery.FieldsToFetch <- [|AggregateStatePersistence.wakeupTimeFieldName; "AggregateType"|]
            indexQuery.SortedFields <- [|new Raven.Abstractions.Data.SortedField(AggregateStatePersistence.wakeupTimeFieldName)|]
            indexQuery.Start <- start
            indexQuery.PageSize <- 200
            let! result = dbCommands.QueryAsync(AggregateStatePersistence.wakeupIndexName, indexQuery, Array.empty) |> Async.AwaitTask
            
            for result in result.Results do
                let documentKey = (result.Item "__document_id").Value<string>()
                let streamId = documentKey.Replace(AggregateStatePersistence.documentKeyPrefix.ToLowerInvariant(), "")
                let wakeupTime = DateTime.Parse((result.Item AggregateStatePersistence.wakeupTimeFieldName).Value<string>())
                let aggregateType = getAggregateType (result.Item "AggregateType")
                yield (streamId, aggregateType, wakeupTime)
            
            if result.TotalResults > result.SkippedResults + result.Results.Count then
                yield! loop (start + result.Results.Count)
        }

        loop 0

    let agent = Agent.Start(fun agent -> 
            let rec loop () = async {
                let! msg = agent.Receive()
                    
                do! 
                    getWakeups DateTime.UtcNow
                    |> AsyncSeq.iter (fun (streamId, aggregateType, wakeupTime) -> onStreamWakeup streamId aggregateType wakeupTime)
                return! loop ()
            }
                
            loop ()
        )

    let callback _ = agent.Post TimerTick

    interface IWakeupMonitor with
        member x.Start () = 
            let timer = new Timer(callback, null, TimeSpan.Zero, TimeSpan.FromSeconds(1.0));
            ()