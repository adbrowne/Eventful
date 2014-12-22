namespace BookLibrary

open System.Threading.Tasks
open Eventful
open Eventful.EventStore
open System

type IBookLibrarySystem =
    abstract member RunCommand<'a> : 'a -> Async<CommandResult<IEvent,BookLibraryEventMetadata>>
    abstract member RunCommandTask<'a> : 'a -> Task<CommandResult<IEvent,BookLibraryEventMetadata>>

type BookLibraryEventStoreSystem = EventStoreSystem<unit,BookLibraryEventContext,BookLibraryEventMetadata,IEvent,AggregateType>

open System.ComponentModel
[<CLIMutable>]
[<TypeConverterAttribute(typeof<IdTypeConverter>)>]
type BookId = {
    Id : Guid
} with static member New () = { Id = Guid.NewGuid() }
and IdTypeConverter () = 
    inherit TypeConverter()
    override x.CanConvertFrom(context:ITypeDescriptorContext, sourceType : Type) : bool =
        if sourceType = typeof<string> then
            true
        else
            (x :> TypeConverter).CanConvertFrom(context, sourceType)

    override x.ConvertFrom(context:ITypeDescriptorContext, culture : System.Globalization.CultureInfo , value : obj) : obj =
        match value with
        | :? string as value -> { BookId.Id = Guid.Parse(value) } :> obj
        | _ -> (x :> TypeConverter).ConvertFrom(context, culture, value)

    override x.ConvertTo(context : ITypeDescriptorContext, culture : System.Globalization.CultureInfo, value : obj, destinationType : Type) : obj =  
        if destinationType = typeof<string> then
            (value :?> BookId).Id.ToString() :> obj
        else
            (x :> TypeConverter).ConvertTo(context, culture, value, destinationType)

             

[<CLIMutable>]
type BookCopyId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type AwardId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type DeliveryId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

[<CLIMutable>]
type FileId = {
    Id : Guid
}
with static member New () = { Id = Guid.NewGuid() }

type IBookEvent = 
    inherit IEvent
    abstract member BookId : BookId

[<CLIMutable>]
type BookCopyAddedEvent = 
    { BookCopyId : BookCopyId
      BookId : BookId }
    interface IBookEvent with
      member x.BookId = x.BookId

[<CLIMutable>]
type BookPromotedEvent = {
    BookId : BookId
}
with interface IEvent

[<CLIMutable>]
type BookPrizeAwardedEvent = {
    AwardId : AwardId
    BookId : BookId
}
with interface IEvent

[<CLIMutable>]
type DeliveryAcceptedEvent = {
    DeliveryId : DeliveryId
    FileId : FileId
}
with interface IEvent

[<CLIMutable>]
type BookDelivery = {
    BookId : BookId
    Title : string
    Copies : int
}

[<CLIMutable>]
type DeliveryDocument = {
    Books : BookDelivery list
}