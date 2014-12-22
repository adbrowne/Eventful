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
       documentStore : Raven.Client.IDocumentStore,
       database : string,
       serializer: ISerializer,
       onStreamWakeup : string -> 'TAggregateType -> DateTime -> unit
    ) =

    let log = createLogger "Eventful.Raven.WakeupMonitor"
    let dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(database)

    let getAggregateType (token : Raven.Json.Linq.RavenJToken) = 
        if typeof<'TAggregateType> = typeof<string> then
            token.Value<string>() :> obj :?> 'TAggregateType
        else
            serializer.DeserializeObj (System.Text.Encoding.UTF8.GetBytes(token.ToString())) typeof<'TAggregateType> :?> 'TAggregateType
        
    let getWakeups time = 
        let rec loop start = asyncSeq {
            log.RichDebug "getWakeups {@Time} {@Start}" [|time;start|]
            let indexQuery = new Raven.Abstractions.Data.IndexQuery()
            indexQuery.FieldsToFetch <- [|AggregateStatePersistence.wakeupTimeFieldName; "AggregateType"|]
            indexQuery.SortedFields <- [|new Raven.Abstractions.Data.SortedField(AggregateStatePersistence.wakeupTimeFieldName)|]
            indexQuery.Start <- start
            indexQuery.PageSize <- 200
            let! result = dbCommands.QueryAsync(AggregateStatePersistence.wakeupIndexName, indexQuery, Array.empty) |> Async.AwaitTask
            log.RichDebug "getWakeups {@Result}" [|result|]
            
            for result in result.Results do
                let documentKey = (result.Item "__document_id").Value<string>()
                let streamId = documentKey.Replace(AggregateStatePersistence.documentKeyPrefix.ToLowerInvariant(), "")
                let wakeupToken = result.Item AggregateStatePersistence.wakeupTimeFieldName
                let wakeupTime = DateTime.Parse(wakeupToken.Value<string>())
                let aggregateType = getAggregateType (result.Item "AggregateType")
                log.RichDebug "Waking up {@StreamId} {@AggregateType} {@Time}" [|streamId;aggregateType;wakeupTime|]
                yield (streamId, aggregateType, wakeupTime)
            
            if result.TotalResults > result.SkippedResults + result.Results.Count then
                yield! loop (start + result.Results.Count)
        }

        loop 0

    let agent = newAgent "WakeupMonitor" log (fun agent -> 
            let rec loop () = async {
                log.RichDebug "agent.Receive()" Array.empty
                let! msg = agent.Receive()

                log.RichDebug "Wakeup tick" Array.empty

                do! 
                    getWakeups DateTime.UtcNow
                    |> AsyncSeq.iter (fun (streamId, aggregateType, wakeupTime) -> onStreamWakeup streamId aggregateType wakeupTime)
                log.RichDebug "Wakeup tick complete" Array.empty
                return! loop ()
            }
                
            loop ()
        )

    let callback _ = agent.Post TimerTick

    interface IWakeupMonitor with
        member x.Start () = 
            log.RichDebug "IWakeupMonitor.Start" Array.empty
            let timer = new Timer(callback, null, TimeSpan.Zero, TimeSpan.FromSeconds(1.0));
            ()