**instructions**

* JDK 25
* https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
* https://interactivebrokers.github.io/

**common commands**

gradle run
gradle run --args="repl"
gradle installDist

cd \\code\\ibhist\\app\\build\\install\\app\\bin  
.\\app.bat repl

running App.main with

* 0 parameters starts IBConnectorImpl.process()
* 1 dummy parameter starts ReplImpl.rum()



\# docker



docker run --name mongo -p 27017:27017 -d mongodb/mongodb-community-server:latest 

docker start mongo

