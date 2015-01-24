namespace Eventful.Testing

open Eventful
open FsCheck
open Swensen.Unquote

module ConfigurationValidation = 
    // will be a huge to find errors in the eventful configuration
    let ``Check configuration`` (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent>) =
        // this used to have some tests but the types were fixed to remove them
        // leaving this stub here so that as things are added in the future 
        // users who have wired this up will get errors
        ()