# Dot-source this once per PowerShell session before running any course code:
#   . .\00-setup\set_env.ps1
#
# This file is a generic template — safe to commit, contains no machine-specific paths.
# Your actual JAVA_HOME/HADOOP_HOME values go in 00-setup/set_env.local.ps1, which is
# gitignored, so your local machine's usernames/paths never get committed to this
# (public) repo. First run: copy set_env.local.ps1.example to set_env.local.ps1 and
# fill in your real paths (see 00-setup/README.md for how to obtain a JDK/winutils).

$localOverride = Join-Path $PSScriptRoot "set_env.local.ps1"

if (Test-Path $localOverride) {
    . $localOverride
    Write-Host "JAVA_HOME -> $env:JAVA_HOME"
    Write-Host "HADOOP_HOME -> $env:HADOOP_HOME"
    Write-Host "Now activate your venv, e.g.: C:\venvs\pyspark-course\Scripts\Activate.ps1"
} else {
    Write-Host "No 00-setup/set_env.local.ps1 found."
    Write-Host "Copy 00-setup/set_env.local.ps1.example to 00-setup/set_env.local.ps1 and fill in your paths."
}
