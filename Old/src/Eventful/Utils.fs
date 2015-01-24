namespace Eventful

module Utils = 
    open System.Reflection
    let getLoadableTypes (assembly : System.Reflection.Assembly) =
        try
            assembly.GetTypes()
        with | :? ReflectionTypeLoadException as e ->
            e.Types
            |> Array.filter ((<>) null)