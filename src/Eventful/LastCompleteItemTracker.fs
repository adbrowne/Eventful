namespace Eventful

type LastCompleteItemTracker<'TItem when 'TItem : equality> private 
    (
        items : Map<int64, 'TItem>, 
        completed: Set<int64>,
        lastCompleteItem : 'TItem option,
        lastStartedIndex : int64,
        lastCompletedIndex: int64
    ) =

    let rec getMinimumCompleted (alreadyCompleted : Set<int64>) (currentMinimum : int64) =
        let next = currentMinimum + 1L
        if (alreadyCompleted |> Set.contains (currentMinimum + 1L)) then
            getMinimumCompleted alreadyCompleted next
        else
            currentMinimum
        
    static let empty =
        new LastCompleteItemTracker<'TItem>(Map.empty, Set.empty, None, -1L, -1L)

    static member Empty = empty

    member x.LastComplete = lastCompleteItem

    member x.Start (item:'TItem) =
        let index = lastStartedIndex + 1L
        let items' = items |> Map.add index item 
        new LastCompleteItemTracker<'TItem>(items', completed, lastCompleteItem, index, lastCompletedIndex)
        
    member x.Complete (item:'TItem) =
        let itemIndex = items |> Map.findKey (fun k v -> v = item)
        let completed' = completed |> Set.add itemIndex
        new LastCompleteItemTracker<'TItem>(items, completed', lastCompleteItem, lastStartedIndex, lastCompletedIndex)

    member x.UpdateLastCompleted () =
        let lastCompletedIndex' = getMinimumCompleted completed lastCompletedIndex
        let lastComplete' =
            if lastCompletedIndex' > -1L then
               items |> Map.find lastCompletedIndex' |> Some
            else
                None
        let items' = items |> Map.filter (fun k v -> k >= lastCompletedIndex')
        let completed' = completed |> Set.filter (fun v -> v > lastCompletedIndex')
        new LastCompleteItemTracker<'TItem>(items', completed', lastComplete', lastStartedIndex, lastCompletedIndex')

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module LastCompleteItemTracker =
    let start<'TItem when 'TItem : equality> (item : 'TItem) (tracker : LastCompleteItemTracker<'TItem>) = 
        tracker.Start item

    let complete<'TItem when 'TItem : equality> (item : 'TItem) (tracker : LastCompleteItemTracker<'TItem>) = 
        tracker.Complete item