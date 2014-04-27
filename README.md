Eventful
========

```fsharp
[<AggregateModule>]
module PersonAggregate = 
    type internal Marker = interface end

    type PersonState = {
        FirstName : string
        LastName : string
    }

    let state = new StateGen<PersonState>((fun s _ -> s), { PersonState.FirstName = ""; LastName = "" }) 

    type CreatePersonCmd = {
        Id : Guid
        FirstName : string
        LastName : string
    }

    [<CommandHandler>]
    let HandleCreatePerson (cmd : CreatePersonCmd, state : PersonState) =
        Handler.Start cmd.Id cmd state
        |> Validate.NonNullProperty "FirstName" cmd.FirstName
        |> Validate.NonNullProperty "LastName" cmd.LastName
        |> Handler.Output 
            {
                PersonCreatedEvt.Id = cmd.Id
                FirstName = cmd.FirstName
                LastName = cmd.LastName
            }
```
