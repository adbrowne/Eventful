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

task AppveyorPostBuild -depends CreateNugetPackages, DownloadEventStore

task RestorePackages {
  exec { & {.\tools\nuget\nuget.exe restore ..\src\Eventful.sln }}
}

task Test -depends MsBuildRelease {
	exec { & { ..\src\packages\xunit.runners.1.9.2\tools\xunit.console.clr4.exe .\Release\Eventful.Tests.dll }}
}

function Expand-ZIPFile($filename, $destinationDirectory)
{
  $shell_app = new-object -com shell.application
  Write-Host $fileName
  $fullZipPath = "$PSScriptRoot\$filename"
  $fullDestinationPath = "$PSScriptRoot\$destinationDirectory"
  Write-Host $fullZipPath
  Write-Host $fullDestinationPath
  $zip_file = $shell_app.namespace($fullZipPath)

  #set the destination directory for the extracts
  if (Test-Path $fullDestinationPath) { $destination = $shell_app.namespace($fullDestinationPath) } else { mkdir $fullDestinationPath ; $destination = $shell_app.namespace($fullDestinationPath)}

  #unzip the file
  $destination.Copyhere($zip_file.items(), 0x14)
}

task DownloadEventStore {
  $executablePath = "$PSScriptRoot\EventStore3\EventStore.ClusterNode.exe"
  if (Test-Path $executablePath){
    # already setup
  }
  else{
    $downloadPath = "$PSScriptRoot\EventStore3.zip"
    $wc=new-object system.net.webclient
    $wc.UseDefaultCredentials = $true
    $wc.downloadfile("http://download.geteventstore.com/binaries/EventStore-OSS-Win-v3.0.0.zip", $downloadPath)

    exec { & {.\tools\7za\7za.exe x .\EventStore3.zip -oEventStore3 -y }}
  }
}

task Package -depends Clean, RestorePackages, MsBuildRelease, CreateNugetPackages {
}

task CreateNugetPackages {
  $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  $version = $version.Substring(0, $version.LastIndexOf("."))
  $version = "$version-beta"

  #exec { & {.\tools\nuget\nuget.exe pack .\packages\Eventful.nuspec -version $version -Verbosity detailed}}
  #Move-Item -force Eventful.$version.nupkg Eventful.nupkg

  exec { & {.\tools\nuget\nuget.exe pack .\packages\Eventful.nuspec -version $version -Verbosity detailed -Symbols }}  
  Move-Item -force Eventful.$version.nupkg Eventful.nupkg
  Move-Item -force Eventful.$version.symbols.nupkg Eventful.symbols.nupkg

  #New-Item -force .\packages\Raven\lib\net45 -itemtype directory
  #Copy-Item .\Release\Eventful.RavenDb.dll .\packages\Raven\lib\net45
  #exec { & {.\tools\nuget\nuget.exe pack .\packages\Raven\Eventful.Raven.nuspec -version $version -Verbosity detailed -Properties EventfulVersion=$version}}
  #Move-Item -force Eventful.RavenDb.$version.nupkg RavenDb.nupkg

  #New-Item -force .\packages\EventStore\lib\net45 -itemtype directory
  #Copy-Item .\Release\Eventful.EventStore.dll .\packages\EventStore\lib\net45
  #exec { & {.\tools\nuget\nuget.exe pack .\packages\EventStore\Eventful.EventStore.nuspec -version $version -Verbosity detailed -Properties EventfulVersion=$version}}
  #Move-Item -force Eventful.EventStore.$version.nupkg EventStore.nupkg

  #New-Item -force .\packages\Neo4j\lib\net45 -itemtype directory
  #Copy-Item .\Release\Eventful.Neo4j.dll .\packages\Neo4j\lib\net45
  #exec { & {.\tools\nuget\nuget.exe pack .\packages\Neo4j\Eventful.Neo4j.nuspec -version $version -Verbosity detailed -Properties EventfulVersion=$version}}
  #Move-Item -force Eventful.Neo4j.$version.nupkg Neo4j.nupkg
}

task PackagePush -depends Package {
  $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  $version = "$version-beta"
  exec { & {.\tools\nuget\nuget.exe push Eventful.$version.nupkg }}
}

task ? -Description "Helper to display task info" {
	Write-Documentation
}
