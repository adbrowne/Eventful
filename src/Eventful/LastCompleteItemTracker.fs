namespace Eventful

type LastCompleteItemTracker<'TItem when 'TItem : comparison> private 
    (
        items : Map<'TItem, int64>, 
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
        let items' = items |> Map.add item index
        new LastCompleteItemTracker<'TItem>(items', completed, lastCompleteItem, index, lastCompletedIndex)
        
    member x.Complete (item:'TItem) =
        let itemIndex = items.[item]
        let completed' = completed |> Set.add itemIndex
        let lastCompletedIndex' = getMinimumCompleted completed' lastCompletedIndex
        let lastComplete' = items |> Map.tryFindKey (fun k v -> v = lastCompletedIndex')
        let items' = items |> Map.filter (fun k v -> v >= lastCompletedIndex')
        let completed' = completed' |> Set.filter (fun v -> v > lastCompletedIndex')
        new LastCompleteItemTracker<'TItem>(items', completed', lastComplete', lastStartedIndex, lastCompletedIndex')

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module LastCompleteItemTracker =
    let start<'TItem when 'TItem : comparison> (item : 'TItem) (tracker : LastCompleteItemTracker<'TItem>) = 
        tracker.Start item

    let complete<'TItem when 'TItem : comparison> (item : 'TItem) (tracker : LastCompleteItemTracker<'TItem>) = 
        tracker.Complete item