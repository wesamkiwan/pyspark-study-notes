# Dot-source this once per PowerShell session before running any course code:
#   . .\00-setup\set_env.ps1
#
# These exact values were verified working on this machine while building the course
# (see 00-setup/README.md "Windows gotchas" for why each one is needed). If you're on a
# different machine, install a JDK per the README and update JAVA_HOME/HADOOP_HOME below.

$env:JAVA_HOME = "C:\Users\wesam\tools\jdk-17.0.19+10"
$env:HADOOP_HOME = "C:\Users\wesam\tools\hadoop-3.3.4"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

Write-Host "JAVA_HOME -> $env:JAVA_HOME"
Write-Host "HADOOP_HOME -> $env:HADOOP_HOME"
Write-Host "Now activate your venv, e.g.: C:\venvs\pyspark-course\Scripts\Activate.ps1"
