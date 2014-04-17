// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open Eventful
open EventStore.ClientAPI
open metrics

type TestType = TestType

[<EntryPoint>]
let main argv = 
    let eventsMeter = Metrics.Meter(typeof<TestType>, "event", "events", TimeUnit.Seconds)
    Metrics.EnableConsoleReporting(10L, TimeUnit.Seconds)

    let getStreamId (recordedEvent : ResolvedEvent) =
        recordedEvent.OriginalStreamId

    let batchCounter = new CounterAgent()
    let onItem group items = async {
         do! Async.Sleep(10)
         return ()
    }

    let onComplete item = async {
        return ()
    }

    let queue = new WorktrackingQueue<string, ResolvedEvent, string>(100000, Set.singleton << getStreamId, onComplete, 10000, onItem, (fun (item : ResolvedEvent) -> item.OriginalPosition.Value.ToString()))

    async {
        printfn "Started"
        let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
        let tcs = new System.Threading.Tasks.TaskCompletionSource<unit>()
        let connectionSettingsBuilder = 
            ConnectionSettings.Create().OnConnected(fun _ _ -> printf "Connected"; ).OnErrorOccurred(fun _ ex -> printfn "Error: %A" ex).SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
        let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

        let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)

        connection.Connect()

        let sw = System.Diagnostics.Stopwatch.StartNew()
        let eventAppeared (event:ResolvedEvent) = 
            if(event.OriginalStreamId.StartsWith("$")) then
                ()
            elif (event.OriginalStreamId.Contains("Ping")) then
                ()
            else
                async {
                    do! queue.Add event 
                } |> Async.RunSynchronously
                eventsMeter.Mark()

        connection.SubscribeToAllFrom(System.Nullable(), false, (fun subscription event -> eventAppeared event), (fun subscription -> tcs.SetResult ())) |> ignore

        do! tcs.Task |> Async.AwaitTask
        printfn "All events read"

        do! queue.AsyncComplete()
        printfn "All events complete"

    } |> Async.RunSynchronously
    0
