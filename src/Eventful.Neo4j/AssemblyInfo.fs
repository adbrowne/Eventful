namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Eventful.Neo4j")>]
[<assembly: AssemblyProductAttribute("Eventful")>]
[<assembly: AssemblyDescriptionAttribute("An EventSourcing library")>]
[<assembly: AssemblyVersionAttribute("0.3")>]
[<assembly: AssemblyFileVersionAttribute("0.3")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.3"
