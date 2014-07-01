namespace Eventful.Raven

open Eventful
open System
type HashSet<'T> = System.Collections.Generic.HashSet<'T>

[<CustomEquality; CustomComparison>]
type UntypedDocumentProcessor<'TContext> = {
    ProcessorKey : string
    MatchingKeys: SubscriberEvent<'TContext> -> seq<KeyProcessor<IComparable, 'TContext>>
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
    member x.Add<'TKey,'TDocument when 'TKey :> IComparable and 'TKey : comparison>(processor:DocumentProcessor<'TKey, 'TDocument, 'TEventContext>) =

        let matchingKeysUntyped event =
            processor.MatchingKeys event
            |> Seq.map (fun { Key = k; Process = p} -> { Key = k :> IComparable; Process = p})

        let untypedProcessor = {
            ProcessorKey = "ProcessorFor: " + typeof<'TDocument>.FullName
            MatchingKeys = matchingKeysUntyped
            EventTypes = new HashSet<Type>(processor.EventTypes)
        }

        let processors' = untypedProcessor::processors
        new ProcessorSet<'TEventContext>(processors')

    static member Empty = new ProcessorSet<'TEventContext>(List.empty)