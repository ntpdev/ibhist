param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("day", "hist", "repl", "week")]
    [string]$Mode = "repl"
)

gradle installDist
Push-Location ".\app\build\install\app\bin\"
& .\app.bat $Mode
Pop-Location