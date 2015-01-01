namespace Eventful.Raven

open System
open Eventful
open FSharp.Control
open System.Threading
open Raven.Client.Connection.Async

type WakeupMonitorEvents =
| TimerTick
| IndexChanged

module WakeupMonitorModule =
    let log = createLogger "Eventful.Raven.WakeupMonitor"

    let minWakeupTimeTicks = 
        UtcDateTime.minValue
        |> UtcDateTime.toString

    let getWakeups waitForNonStale (dbCommands : IAsyncDatabaseCommands) (time : UtcDateTime) = 
        let rec loop start = asyncSeq {
            log.RichDebug "getWakeups {@Time} {@Start}" [|time;start|]
            let indexQuery = new Raven.Abstractions.Data.IndexQuery()
            indexQuery.FieldsToFetch <- [|AggregateStatePersistence.wakeupTimeFieldName; "AggregateType"; AggregateStatePersistence.streamIdFieldName|]
            indexQuery.SortedFields <- [|new Raven.Abstractions.Data.SortedField(AggregateStatePersistence.wakeupTimeFieldName)|]
            indexQuery.Start <- start
            indexQuery.PageSize <- 200
            indexQuery.Query <- sprintf "WakeupTime: [\"%s\" TO \"%s\"]" minWakeupTimeTicks (time |> UtcDateTime.toString)
            let! result = dbCommands.QueryAsync(AggregateStatePersistence.wakeupIndexName, indexQuery, Array.empty) |> Async.AwaitTask
            log.RichDebug "getWakeups {@Result}" [|result|]

            if result.IsStale && waitForNonStale then
                yield! loop start
            else 
                for result in result.Results do
                    let streamId = (result.Item "StreamId").Value<string>()
                    let wakeupToken = result.Item AggregateStatePersistence.wakeupTimeFieldName
                    let wakeupTime = wakeupToken.Value<string>() |> UtcDateTime.fromString
                    let aggregateType = (result.Item "AggregateType").Value<string>()
                    log.RichDebug "Waking up {@StreamId} {@AggregateType} {@Time}" [|streamId;aggregateType;wakeupTime|]
                    yield (streamId, aggregateType, wakeupTime)
                
                if result.TotalResults > result.SkippedResults + result.Results.Count then
                    yield! loop (start + result.Results.Count)
        }

        loop 0

type WakeupMonitor
    (
       documentStore : Raven.Client.IDocumentStore,
       database : string,
       onStreamWakeup : string -> string -> UtcDateTime -> unit
    ) =

    let log = createLogger "Eventful.Raven.WakeupMonitor"
    let dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(database)

    let getWakeups = WakeupMonitorModule.getWakeups false dbCommands

    let agent = newAgent "WakeupMonitor" log (fun agent -> 
            let rec loop () = async {
                let! (msg : WakeupMonitorEvents) = agent.Receive()

                log.RichDebug "Wakeup tick" Array.empty

                do! 
                    getWakeups (DateTime.UtcNow |> UtcDateTime.fromDateTime)
                    |> AsyncSeq.iter (fun (streamId, aggregateType, wakeupTime) -> onStreamWakeup streamId aggregateType wakeupTime)
                return! loop ()
            }
                
            loop ()
        )

    let callback _ = 
        try
            agent.Post TimerTick
        with | e ->
            log.ErrorWithException <| lazy("Exception in timer callback", e)

    // create timer but not running
    let timer = new Timer(callback, null, -1, -1)

    interface IWakeupMonitor with
        member x.Start () = 
            log.RichDebug "IWakeupMonitor.Start" Array.empty
            let timerUpdated = timer.Change(TimeSpan.Zero, TimeSpan.FromSeconds(1.0))
            if not timerUpdated then
                log.RichError "Failed to start timer" Array.empty
            ()

        member x.Stop () =
            let timerUpdated = timer.Change(-1, -1)
            if not timerUpdated then
                log.RichError "Failed to stop timer" Array.empty