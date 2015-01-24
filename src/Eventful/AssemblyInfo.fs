namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Eventful")>]
[<assembly: AssemblyProductAttribute("Eventful")>]
[<assembly: AssemblyDescriptionAttribute("An EventSourcing library")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
