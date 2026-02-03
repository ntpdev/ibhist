## instructions

* JDK 25
* https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
* https://interactivebrokers.github.io/

## gradle commands

gradle run
gradle run --args="repl"
gradle installDist

cd \\code\\ibhist\\app\\build\\install\\app\\bin  
.\\app.bat repl

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
