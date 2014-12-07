namespace BookLibrary

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

    let buildAwardMetadata = 
        Aggregates.emptyMetadata AggregateType.Award 

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
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            Seq.empty

open Suave.Http
open Suave.Http.Applicatives
open BookLibrary.WebHelpers

module AwardsWebApi = 
    let addHandler (cmd : AddBookPrizeAwardCommand) =
        let awardId = AwardId.New()
        let cmd = { cmd with AwardId = awardId }
        let successResponse = 
            let responseBody = new Newtonsoft.Json.Linq.JObject();
            responseBody.Add("awardId", new Newtonsoft.Json.Linq.JValue(awardId))
            responseBody
        (cmd, successResponse :> obj)

    let config system =
        choose [
            url "/api/awards" >>= choose
                [ 
                    POST >>= commandHandler system addHandler
                ]
        ]