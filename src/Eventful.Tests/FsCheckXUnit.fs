namespace Eventful.Tests

open Xunit
open FsCheck

module FsCheckXUnit = 
    let xUnitRunner =
        { new IRunner with
            member x.OnStartFixture t = ()
            member x.OnArguments (ntest,args, every) = ()
            member x.OnShrink(args, everyShrink) = ()
            member x.OnFinished(name,testResult) = 
                match testResult with 
                | TestResult.True _ -> Assert.True(true)
                | _ -> Assert.True(false, Runner.onFinishedToString name testResult) 
        }

    let withxUnitConfig = { Config.Default with Runner = xUnitRunner }