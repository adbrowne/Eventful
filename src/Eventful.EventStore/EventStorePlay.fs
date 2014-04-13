module EventStorePlay

    open Eventful
    open NUnit.Framework
    open EventStore.ClientAPI

    [<Test>]
    let ``Play eventstore events`` () : unit = 
        let consumer group (eventList:System.Collections.Generic.List<RecordedEvent>) =
            async {
                    printfn "Group: %s, Count: %d" group eventList.Count
            }

        let queue = new GroupingBoundedQueue<string,RecordedEvent>(1000, 100000, 10, consumer)

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

            connection.SubscribeToAllFrom(System.Nullable(), false, (fun subscription event -> eventAppeared event)) |> ignore        

            printfn "Subscribed"
            do! tcs.Task |> Async.AwaitTask
        } |> Async.RunSynchronously