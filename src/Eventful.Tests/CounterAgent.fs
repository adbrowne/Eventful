namespace Eventful.Tests

open Eventful

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