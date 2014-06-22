properties {
  $slnPath = "..\src\Eventful.sln"
}

task default -depends MsBuildRelease

task Clean {
  if (Test-Path .\package\lib) {
  	remove-item .\package\lib -force -recurse
  }
  remove-item Eventful*.nupkg
  exec { msbuild /t:Clean $slnPath }
}

task MsBuildDebug {
  exec { msbuild /t:Build $slnPath -p:Configuration=Debug /maxcpucount:8 /verbosity:quiet}
}

task MsBuildRelease {
  exec { msbuild /t:Build $slnPath -p:Configuration=Release /maxcpucount:8 /verbosity:quiet }
}

task RestorePackages {
  exec { & {.\tools\nuget\nuget.exe restore ..\src\Eventful.sln }}
}

task Test -depends MsBuildRelease {
	exec { & { ..\src\packages\xunit.runners.1.9.2\tools\xunit.console.clr4.exe .\Release\Eventful.Tests.dll }}
}

task Package -depends Clean, RestorePackages, MsBuildRelease, CreateNugetPackage {
}

task CreateNugetPackage {
  $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  New-Item -force .\package\lib\net45 -itemtype directory
  Copy-Item .\Release\Eventful.dll .\package\lib\net45
  Copy-Item .\Release\Eventful.EventStore.dll .\package\lib\net45
  exec { & {.\tools\nuget\nuget.exe pack .\package\Eventful.nuspec -version $version }}
  Copy-Item Eventful.$version.nupkg output.nupkg
}

task PackagePush -depends Package {
  $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  exec { & {.\tools\nuget\nuget.exe push Eventful.$version.nupkg }}
}

task ? -Description "Helper to display task info" {
	Write-Documentation
}