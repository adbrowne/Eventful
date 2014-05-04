namespace Eventful.New

type Agent<'T> = MailboxProcessor<'T>
open FSharpx.Collections
type BoundedWorkCounter = Eventful.BoundedWorkCounter
type LastCompleteTracker<'TState, 'TMsg> = Eventful.LastCompleteTracker<'TState, 'TMsg>

type GroupAgentMessages<'TGroup, 'TItem> = 
| Enqueue of (int64 * 'TItem)
| ConsumeBatch of (int64 * ('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)
| BatchComplete

type WorkQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
| ItemGroups of (int64 * Set<'TGroup>)
| WorkGrouped of (int64 * 'TGroup)
| QueueWork of Agent<GroupAgentMessages<'TGroup, 'TItem>>
| ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)

type ItemAgentMessages<'TGroup when 'TGroup : comparison> =
| Complete of 'TGroup
| NotifyWhenComplete of Async<unit>
| Shutdown

type CompletionAgentMessages<'TGroup, 'TItem when 'TGroup : comparison> =
| ItemStart of (int64 * Set<'TGroup>)
| ItemComplete of (int64 * 'TGroup)
| AllComplete of (int64 * AsyncReplyChannel<unit>)

type GroupCompleteState<'TGroup when 'TGroup : comparison> =
| Completed of Set<'TGroup>
| Remaining of Set<'TGroup>

type GroupCompleteTrackerMessages<'TGroup when 'TGroup : comparison> =
| TrackerStart of (int64 * Set<'TGroup>)
| TrackerComplete of (int64 * 'TGroup)

type GroupsCompleteTracker<'TGroup, 'TItem when 'TGroup : comparison> private (tracker : LastCompleteTracker<GroupCompleteState<'TGroup>,GroupCompleteTrackerMessages<'TGroup>>) =
    static let mapping (msg : GroupCompleteTrackerMessages<'TGroup>, state : GroupCompleteState<'TGroup> option) = 
        match (msg, state) with
        | (TrackerStart (id, groups), None) -> Some <| Remaining groups
        | (TrackerStart (id, groups), Some (Completed alreadyCompleted)) -> 
            let remainingGroups = groups |> Set.difference alreadyCompleted
            if(remainingGroups = Set.empty) then
                None
            else
                Some <| Remaining remainingGroups
        | (TrackerComplete (id, group), None) -> Some <| Completed (Set.singleton group)
        | (TrackerComplete (id, group), Some (Completed alreadyCompleted)) -> 
            Some <| Completed (alreadyCompleted |> Set.add group)
        | (TrackerComplete (id, group), Some (Remaining remaining)) -> 
            let remaining' = remaining |> Set.remove group
            if(remaining' = Set.empty) then
                None
            else
                Some <| Remaining remaining'
        | (_, state) -> state // this is an error

    static let empty = 
        new GroupsCompleteTracker<'TGroup,'TItem>(LastCompleteTracker<GroupCompleteState<'TGroup>,GroupCompleteTrackerMessages<'TGroup>>.Empty mapping)

    static member Empty = empty

    member x.LastComplete = tracker.LastComplete

    member x.Process operation = 
        let id = 
            match operation with
            | TrackerStart (id,_) -> id
            | TrackerComplete (id,_) -> id

        let (isComplete, updated) = tracker.Process id operation
        (isComplete, new GroupsCompleteTracker<'TGroup,'TItem>(updated))

type OrderedGroupingQueue<'TGroup, 'TItem  when 'TGroup : comparison>() =

    let maxItems = 1000

    let log (value : string) =
        //Console.WriteLine value
        ()

    let workQueueAgent = Agent.Start(fun agent -> 
        let rec empty (groupsCompleteTracker : GroupsCompleteTracker<'TGroup, 'TItem>) = 
            agent.Scan(fun msg -> 
            match msg with
            | QueueWork agent -> Some(enqueue agent Queue.empty groupsCompleteTracker)
            | ItemGroups (itemIndex, groups) -> 
                let (isComplete, tracker') = groupsCompleteTracker.Process (TrackerStart (itemIndex, groups))
                Some(empty (tracker'))
            | WorkGrouped (itemIndex, group) -> 
                let (isComplete, tracker') = groupsCompleteTracker.Process (TrackerComplete (itemIndex, group))
                Some(empty (tracker'))
            | _ -> None) 
        and hasWork (queue : Queue<Agent<GroupAgentMessages<'TGroup, 'TItem>>>) (groupsCompleteTracker : GroupsCompleteTracker<'TGroup, 'TItem>) = async {
            let! msg = agent.Receive()
            // Console.WriteLine(sprintf "MyQueue receiving %A" msg)

            match msg with
            | QueueWork agent -> return! enqueue agent queue groupsCompleteTracker
            | ItemGroups (itemIndex, groups) -> 
                let (isComplete, tracker') = groupsCompleteTracker.Process (TrackerStart (itemIndex, groups))
                return! hasWork queue (tracker')
            | WorkGrouped (itemIndex, group) -> 
                let (isComplete, tracker') = groupsCompleteTracker.Process (TrackerComplete (itemIndex, group))
                return! hasWork queue (tracker')
            | ConsumeWork(work, reply) -> 
                let (next, remaining) = queue |> Queue.uncons 
                next.Post (ConsumeBatch(groupsCompleteTracker.LastComplete,work,reply))
                if(Queue.isEmpty remaining) then
                    return! empty groupsCompleteTracker
                else
                    return! hasWork remaining groupsCompleteTracker}
        and enqueue groupAgent queue groupsCompleteTracker = async {
            // Console.WriteLine(sprintf "Queueing %A" groupAgent)
            return! hasWork (queue |> Queue.conj groupAgent) groupsCompleteTracker
        }
            
        empty GroupsCompleteTracker.Empty
    )

    let buildGroupAgent group (completionAgent:Agent<CompletionAgentMessages<'TGroup, 'TItem>>) = Agent.Start(fun agent -> 
        let rec loop running waiting = async {
            let! msg = agent.Receive()
            match msg with
            | Enqueue (itemIndex, item) -> return! enqueue(itemIndex, item, running, waiting)
            | ConsumeBatch(maxIndex, work, reply) -> return! consumeBatch(maxIndex, work, reply, running, waiting)
            | BatchComplete -> return! batchComplete(waiting) }
        and enqueue(itemIndex, item, running, waiting) = async {
            if ((not running) && (List.isEmpty waiting)) then
               workQueueAgent.Post(WorkGrouped (itemIndex, group))
               workQueueAgent.Post(QueueWork agent)
            else
                ()
            return! (loop running (waiting |> List.cons (itemIndex, item))) }
        and consumeBatch(maxIndex, work, reply, running, waiting) = async {
            let (readyItems, remainingItems) = waiting |> List.partition (fun (index, item) -> index <= maxIndex) 
            async {
               do! work(group, readyItems |> Seq.map snd)
               reply.Reply()
               agent.Post BatchComplete
               for (itemIndex, item) in readyItems do
                completionAgent.Post(ItemComplete (itemIndex, group))
            } |> Async.Start
            return! loop true remainingItems }
        and batchComplete(waiting) = async {
            // log <| sprintf "Batch complete. Waiting: %A" waiting
            if (waiting |> List.isEmpty) then
                return! loop false waiting
            else
                workQueueAgent.Post(QueueWork agent)
                return! loop false waiting }

        loop false List.empty
    )

    let rec boundedCounter = new BoundedWorkCounter(10000)
    and completionAgent = Agent.Start(fun agent -> 
        let rec run (tracker : GroupsCompleteTracker<'TGroup,'TItem>) toNotify = async { 
            let! msg = agent.Receive()
            match msg with
            | ItemStart (itemIndex, groups) -> return! (itemStart itemIndex groups tracker toNotify)
            | ItemComplete (itemIndex, group) -> return! (itemComplete itemIndex group tracker toNotify) 
            | AllComplete (index,r) -> return! (allComplete index r tracker toNotify)}
        and itemStart itemIndex groups tracker toNotify = async {
            let (isComplete, tracker') = tracker.Process(TrackerStart (itemIndex, groups))
            if isComplete then
                boundedCounter.WorkComplete(1)
            else
                ()
            return! notifyBatchComplete tracker' toNotify }
        and allComplete index r tracker toNotify = async {
            return! notifyBatchComplete tracker ((index, r) :: toNotify) }
        and itemComplete itemIndex group tracker toNotify = async {
            let (isComplete, tracker') = tracker.Process(TrackerComplete (itemIndex, group))
            if isComplete then
                boundedCounter.WorkComplete()
            else
                ()
            return! notifyBatchComplete tracker' toNotify }
        and notifyBatchComplete tracker toNotify = async {
            let (doneCompletions, remainingCompletionSets) = 
                toNotify 
                |> List.partition (fun (itemIndex, callback) -> itemIndex <= tracker.LastComplete)

            for (_, callback) in doneCompletions do
                callback.Reply()
            return! run tracker remainingCompletionSets
        }

        run GroupsCompleteTracker.Empty List.empty )
    and dispatcherAgent = Agent.Start(fun agent -> 
        let ensureGroupAgent groupAgents group =
            if (groupAgents |> Map.containsKey group) then 
                groupAgents 
            else 
                groupAgents |> Map.add group (buildGroupAgent group completionAgent)

        let rec run(groupAgents) = async {
            let! (itemIndex, item, groups) = agent.Receive()
            let newGroupAgents = groups |> Set.fold ensureGroupAgent groupAgents
            completionAgent.Post (ItemStart (itemIndex, groups))
            workQueueAgent.Post(ItemGroups(itemIndex, groups))
            for group in groups do
                let groupAgent = newGroupAgents.[group]
                groupAgent.Post(Enqueue (itemIndex, item))

            return! run(newGroupAgents)
        }

        run (Map.empty) )

    member this.Add (input:'TInput, group: ('TInput -> ('TItem * Set<'TGroup>))) =
        async {
            let! itemIndex = boundedCounter.Start 1
            async {
                let (item, groups) = group input
                dispatcherAgent.Post(itemIndex, item, groups)
            } |> Async.Start
        }

    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        async {
            do! workQueueAgent.PostAndAsyncReply((fun ch -> ConsumeWork(work,ch)))
        }

    member this.CurrentItemsComplete () = 
        async {
            let! completeUpTo = boundedCounter.GetCurrentIndex ()
            return! completionAgent.PostAndAsyncReply(fun ch -> AllComplete (completeUpTo, ch))
        }
