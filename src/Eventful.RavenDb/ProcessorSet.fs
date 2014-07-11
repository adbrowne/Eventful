namespace Eventful.Raven

open Eventful
open System
type HashSet<'T> = System.Collections.Generic.HashSet<'T>

[<CustomEquality; CustomComparison>]
type UntypedDocumentProcessor<'TMessage> = {
    ProcessorKey : string
    Process : IDocumentFetcher -> obj -> seq<'TMessage> -> Async<seq<ProcessAction>>
    MatchingKeys: 'TMessage -> seq<IComparable>
    EventTypes : HashSet<Type>
}
with
    static member Key p = 
        let {ProcessorKey = key } = p
        key
    override x.Equals(y) = 
        equalsOn UntypedDocumentProcessor<'TMessage>.Key x y
    override x.GetHashCode() = 
        hashOn UntypedDocumentProcessor<'TMessage>.Key x
    interface System.IComparable with 
        member x.CompareTo y = compareOn UntypedDocumentProcessor<'TMessage>.Key x y

type ProcessorSet<'TMessage>(processors : List<UntypedDocumentProcessor<'TMessage>>) =
    member x.Items = processors
    member x.Add<'TKey,'TDocument>(processor:DocumentProcessor<'TKey, 'TDocument, 'TMessage>) =
        
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
        new ProcessorSet<'TMessage>(processors')

    static member Empty = new ProcessorSet<'TMessage>(List.empty)