namespace Eventful.Tests

open Eventful
open Xunit
open FsUnit.Xunit

type OperationSetComplete<'T when 'T : comparison> (operationSet : Set<'T>, callback : Async<unit>) =
    let log = createLogger "Eventful.OperationSetComplete"

    let agent = newAgent "OperationSetComplete" log (fun agent ->
        let rec loop state = async {
            let! item = agent.Receive()

            let state' = state |> Set.remove item

            if state'.IsEmpty then
                do! callback
            else
                return! loop state' 
        }
         
        loop operationSet   
    )
    member this.Complete (item : 'T) =
        agent.Post item

module SimpleSetTrackerAgentTests = 

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``When single item is completed then callback is run`` () : unit =
        let isDone = ref false
        let operationTracker = new OperationSetComplete<int>(Set.singleton 1, async { isDone := true })

        operationTracker.Complete(1)

        System.Threading.Thread.Sleep(100)

        !isDone |> should equal true