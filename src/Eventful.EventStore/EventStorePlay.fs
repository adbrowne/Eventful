module EventStorePlay

    open Eventful
    open NUnit.Framework
    open EventStore.ClientAPI
    open metrics

    type TestType = TestType
    [<Test>]
    let ``Play eventstore events without queue`` () : unit = 
        let eventsMeter = Metrics.Meter(typeof<TestType>, "event", "events", TimeUnit.Seconds)
        Metrics.EnableConsoleReporting(10L, TimeUnit.Seconds)
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
                async {
                    return ()
                } |> Async.RunSynchronously
                eventsMeter.Mark()

            connection.SubscribeToAllFrom(System.Nullable(), false, (fun subscription event -> eventAppeared event), (fun subscription -> tcs.SetResult ())) |> ignore

            do! tcs.Task |> Async.AwaitTask
            printfn "All events read"

            printfn "All events complete"

        } |> Async.RunSynchronously


    [<Test>]
    let ``Play eventstore events to null agent`` () : unit = 
        let eventsMeter = Metrics.Meter(typeof<TestType>, "event", "events", TimeUnit.Seconds)
        Metrics.EnableConsoleReporting(10L, TimeUnit.Seconds)
        async {
            printfn "Started"
            let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
            let tcs = new System.Threading.Tasks.TaskCompletionSource<unit>()
            let connectionSettingsBuilder = 
                ConnectionSettings.Create().OnConnected(fun _ _ -> printf "Connected"; ).OnErrorOccurred(fun _ ex -> printfn "Error: %A" ex).SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)

            connection.Connect()

            let agent = Agent.Start(fun (agent:Agent<AsyncReplyChannel<unit>>) -> 
                let rec work() = async {
                    let! msg = agent.Receive()
                    msg.Reply()
                    return! work()
                }
                work()
            )

            let sw = System.Diagnostics.Stopwatch.StartNew()
            let eventAppeared (event:ResolvedEvent) = 
                agent.PostAndAsyncReply((fun ch -> ch)) |> Async.RunSynchronously
                eventsMeter.Mark()

            connection.SubscribeToAllFrom(System.Nullable(), false, (fun subscription event -> eventAppeared event), (fun subscription -> tcs.SetResult ())) |> ignore

            do! tcs.Task |> Async.AwaitTask
            printfn "All events read"

            printfn "All events complete"

        } |> Async.RunSynchronously

    [<Test>]
    let ``Play eventstore events through work tracking queue`` () : unit = 
        let eventsMeter = Metrics.Meter(typeof<TestType>, "event", "events", TimeUnit.Seconds)
        Metrics.EnableConsoleReporting(10L, TimeUnit.Seconds)

        let getStreamId (recordedEvent : ResolvedEvent) =
            recordedEvent.OriginalStreamId

        let batchCounter = new CounterAgent()
        let onItem group items = async {
             // batchCounter.Incriment() |> Async.Start
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

    [<Test>]
    let ``Play eventstore events through grouping queue`` () : unit = 
        let eventsMeter = Metrics.Meter(typeof<TestType>, "event", "events", TimeUnit.Seconds)
        Metrics.EnableConsoleReporting(10L, TimeUnit.Seconds)

        let queue = new GroupingBoundedQueue<string, RecordedEvent, unit>(100000)

        let worker () = 
            async {
                let rec readQueue () = async{
                        do! queue.AsyncConsume ((fun (group, eventList) -> async { 
                               // System.Console.WriteLine(sprintf "Group: %s, Count: %d" group eventList.Length)
                               do! Async.Sleep (1)
                             }))
                        do! readQueue ()
                    }

                do! readQueue()
            }

        Seq.init 1000 (fun _ -> worker ()) |> Async.Parallel |> Async.Ignore |> Async.Start

        async {
            printfn "Started"
            let ipEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
            let tcs = new System.Threading.Tasks.TaskCompletionSource<unit>()
            let connectionSettingsBuilder = 
                ConnectionSettings.Create().OnConnected(fun _ _ -> printf "Connected"; ).OnErrorOccurred(fun _ ex -> printfn "Error: %A" ex).SetDefaultUserCredentials(new SystemData.UserCredentials("admin", "changeit"))
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)

            connection.Connect()
            let eventAppeared (event:ResolvedEvent) = 
                queue.AsyncAdd (event.OriginalStreamId, event.Event) |> Async.RunSynchronously
                eventsMeter.Mark()

            connection.SubscribeToAllFrom(System.Nullable(), false, (fun subscription event -> eventAppeared event)) |> ignore        

            printfn "Subscribed"
            do! tcs.Task |> Async.AwaitTask
        } |> Async.RunSynchronously