namespace BookLibrary

open Xunit
open Eventful.Testing
open NSubstitute

module ConfigurationTests =
    [<Fact>]
    [<Trait("Category","unit")>]
    let handlerConfigruation () =
        let handlers = SetupHelpers.handlers (fun () -> Substitute.For<Raven.Client.IAsyncDocumentSession>())
        ConfigurationValidation.``Check configuration`` handlers
        ()