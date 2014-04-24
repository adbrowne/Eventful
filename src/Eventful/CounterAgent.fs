namespace Eventful

type CounterCmd = 
    | Incriment of (int * AsyncReplyChannel<unit>)
    | Get of AsyncReplyChannel<int>

type CounterAgent () =
    let agent = Agent.Start(fun agent ->

        let rec loop(count) =
            agent.Scan(fun msg -> 
             match msg with
             | Incriment (increment, ch) -> 
                ch.Reply()
                Some(loop (count + increment))
             | Get ch -> 
                ch.Reply count
                Some(loop count))

        loop 0
    )

    member this.Incriment count =
        agent.PostAndAsyncReply(fun ch -> Incriment (count, ch))

    member this.Get () =
        agent.PostAndAsyncReply(fun ch -> Get ch)