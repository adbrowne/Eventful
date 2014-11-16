namespace BookLibrary

open System
open Eventful
open BookLibrary.Aggregates

[<CLIMutable>]
type AddBookPrizeAwardCommand = {
    [<GeneratedIdAttribute>]AwardId : AwardId
    BookId : BookId
}

module Award =
    let getStreamName () (awardId : AwardId) =
        sprintf "Award-%s" <| awardId.Id.ToString("N")

    let getEventStreamName (context : UnitEventContext) (awardId : AwardId) =
        sprintf "Award-%s" <| awardId.Id.ToString("N")

    let inline getAwardId (a: ^a) _ = 
        (^a : (member AwardId: AwardId) (a))

    let getBookAwardIdFromMetadata = (fun (x : BookLibraryEventMetadata) -> { AwardId.Id = x.AggregateId })

    let inline buildAwardMetadata (awardId : AwardId) = 
        Aggregates.emptyMetadata awardId.Id AggregateType.Award 

    let inline awardCmdHandler f = 
        cmdHandler f buildAwardMetadata

    let cmdHandlers = 
        seq {
           let addAward (cmd : AddBookPrizeAwardCommand) =
               { 
                   BookPrizeAwardedEvent.AwardId = cmd.AwardId
                   BookId = cmd.BookId
               }

           yield awardCmdHandler addAward
        }

    let handlers () =
        Eventful.Aggregate.toAggregateDefinition 
            AggregateType.Award 
            BookLibraryEventMetadata.GetUniqueId
            getBookAwardIdFromMetadata
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            Seq.empty

open System.Web
open System.Net.Http
open System.Web.Http
open System.Web.Http.Routing
open FSharpx.Choice
open Eventful
open FSharpx.Collections

[<RoutePrefix("api/awards")>]
type AwardsController(system : IBookLibrarySystem) =
    inherit ApiController()
 
    // POST /api/values
    [<Route("")>]
    [<HttpPost>]
    member x.Post (cmd:AddBookPrizeAwardCommand) = 
        async {
            let awardId = AwardId.New()
            let cmdWithId = { cmd with AwardId = awardId }
            let! cmdResult = system.RunCommand cmdWithId 
            return
                match cmdResult with
                | Choice1Of2 result ->
                     let responseBody = new Newtonsoft.Json.Linq.JObject();
                     responseBody.Add("awardId", new Newtonsoft.Json.Linq.JValue(awardId.Id))
                     let response = x.Request.CreateResponse<Newtonsoft.Json.Linq.JObject>(Net.HttpStatusCode.Accepted, responseBody)
                     match result.Position with
                     | Some position ->
                         response.Headers.Add("eventful-last-write", position.BuildToken())
                     | None ->
                         ()
                     response
                | Choice2Of2 errorResult ->
                     let response = x.Request.CreateResponse<NonEmptyList<CommandFailure>>(Net.HttpStatusCode.BadRequest, errorResult)
                     response
        } |> Async.StartAsTask