namespace Eventful.Tests

open Eventful
open Xunit

type CounterCmd = 
    | Incriment of AsyncReplyChannel<unit>
    | Get of AsyncReplyChannel<int>

type CounterAgent () =
    let agent = Agent.Start(fun agent ->

        let rec loop(count) =
            agent.Scan(fun msg -> 
             match msg with
             | Incriment ch -> 
                ch.Reply()
                Some(loop (count + 1))
             | Get ch -> 
                ch.Reply count
                Some(loop count))

        loop 0
    )

    member this.Incriment () =
        agent.PostAndAsyncReply(fun ch -> Incriment ch)

    member this.Get () =
        agent.PostAndAsyncReply(fun ch -> Get ch)

module CounterAgentTests =

    [<Fact(Skip = "Slow performance test")>]
    let ``Time to count to 1,000,000`` () : unit =
        let counter = new CounterAgent()
        for _ in [0..1000000] do
            counter.Incriment() |> Async.RunSynchronously
     