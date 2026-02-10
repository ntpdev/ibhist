## instructions

* JDK 25
* https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
* https://interactivebrokers.github.io/

## gradle commands

gradle run
gradle run --args repl
gradle installDist

```powershell
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("day", "repl")]
    [string]$Mode
)

gradle installDist
Push-Location ".\app\build\install\app\bin\"
& .\app.bat $Mode
Pop-Location
```

use bat to launch repl to avoid codepage issue when going via gradle

running App.main with

* 0 parameters starts IBConnectorImpl.process()
* 1 dummy parameter starts ReplImpl.rum()

## docker (using name = mongo)

docker run --name mongo -p 27017:27017 -d mongodb/mongodb-community-server:latest 

docker start mongo

docker exec -it mongo mongosh

### example queries in mongosh
use futures
show collections
db.daily.find({symbol:"esh6"}).sort({tradeDate:-1}).limit(5)
