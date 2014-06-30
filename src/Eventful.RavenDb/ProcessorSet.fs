namespace Eventful.Raven

open Eventful
open System
type HashSet<'T> = System.Collections.Generic.HashSet<'T>

[<CustomEquality; CustomComparison>]
type UntypedDocumentProcessor<'TContext> = {
    ProcessorKey : string
    Process : IDocumentFetcher -> obj -> seq<SubscriberEvent<'TContext>> -> Async<seq<DocumentWriteRequest>>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<IComparable>
    EventTypes : HashSet<Type>
}
with
    static member Key p = 
        let {ProcessorKey = key } = p
        key
    override x.Equals(y) = 
        equalsOn UntypedDocumentProcessor<'TContext>.Key x y
    override x.GetHashCode() = 
        hashOn UntypedDocumentProcessor<'TContext>.Key x
    interface System.IComparable with 
        member x.CompareTo y = compareOn UntypedDocumentProcessor<'TContext>.Key x y

type ProcessorSet<'TEventContext>(processors : List<UntypedDocumentProcessor<'TEventContext>>) =
    member x.Items = processors
    member x.Add<'TKey,'TDocument>(processor:DocumentProcessor<'TKey, 'TDocument, 'TEventContext>) =
        
        let processUntyped (fetcher:IDocumentFetcher) (untypedKey : obj) events =
            let key = untypedKey :?> 'TKey
            processor.Process key fetcher events

        let matchingKeysUntyped event =
            processor.MatchingKeys event
            |> Seq.cast<IComparable>

        let untypedProcessor = {
            ProcessorKey = "ProcessorFor: " + typeof<'TDocument>.FullName
            Process = processUntyped
            MatchingKeys = matchingKeysUntyped
            EventTypes = new HashSet<Type>(processor.EventTypes)
        }

        let processors' = untypedProcessor::processors
        new ProcessorSet<'TEventContext>(processors')

    static member Empty = new ProcessorSet<'TEventContext>(List.empty)