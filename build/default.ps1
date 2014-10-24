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

task CreateNugetPackages {
  $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  $version = "$version-beta"

  New-Item -force .\packages\Eventful\lib\net45 -itemtype directory
  Copy-Item .\Release\Eventful.dll .\packages\Eventful\lib\net45
  exec { & {.\tools\nuget\nuget.exe pack .\packages\Eventful\Eventful.nuspec -version $version -Verbosity detailed}}
  Move-Item -force Eventful.$version.nupkg Eventful.nupkg

  New-Item -force .\packages\Raven\lib\net45 -itemtype directory
  Copy-Item .\Release\Eventful.RavenDb.dll .\packages\Raven\lib\net45
  exec { & {.\tools\nuget\nuget.exe pack .\packages\Raven\Eventful.Raven.nuspec -version $version -Verbosity detailed}}
  Move-Item Eventful.Raven.$version.nupkg Raven.nupkg
  # Copy-Item .\Release\Eventful.EventStore.dll .\package\lib\net45
  # Copy-Item .\Release\Eventful.RavenDB.dll .\package\lib\net45
}

task PackagePush -depends Package {
  $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  $version = "$version-beta"
  exec { & {.\tools\nuget\nuget.exe push Eventful.$version.nupkg }}
}

task ? -Description "Helper to display task info" {
	Write-Documentation
}
