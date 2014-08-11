namespace EmergencyRoom

open FsCheck.Xunit

module VisitTests = 

    [<Property>]
    let ``All valid command sequences successfully apply to the Read Model`` () =
        