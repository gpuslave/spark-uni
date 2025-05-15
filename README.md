# Usage

Inside a directory with compose.yml (later - "compose dir") run `docker compose up -d`, this will run our 2 containers in detached mode. 
Now you have two containers named `sbt` and `spark`.  
When you're done you can run `docker compose down` to remove the containers.  
*Note:* If you want to clear SBT system cache (downloaded libs, etc.) you can run `docker compose down -v`, this will remove the containers and also remove volumes connected to SBT.  

### SBT

Build your source code into jar files:

```bash
# log into your sbt container, will start in /app
docker exec -it sbt /bin/bash

# clean build cache
sbt clean

# create Jar files without dependencies
sbt package

# create Uber Jar
sbt assembly

# you can chain these commands also
sbt clean package assembly
```

Your source code and target dirs are mounted to this container, so all of the output will be present on your system too.

### SPARK

You can run Spark two ways: interactive shell and spark-submit.

#### Running with interactive shell

```bash
docker exec -it -u root spark /opt/spark/bin/spark-shell
```
Now you can enter command-by-command to evaluate your expression/program.

#### Running with spark-submit

This method requires you to write your Scala source code using Spark API, compile and package it into `.jar` files.  
*Important note:* If you use any of the external dependencies that are not Spark-related, then you need to `assemble` your code like so `sbt assembly`.  

```bash
# use your name for --class "<YOUR_CLASS>" and your filepath
docker exec -it -u root spark /opt/spark/bin/spark-submit --class "App" --master "local[*]" /app/target/scala-2.12/app-assembly.jar
```

This will run one instance of Spark that will output the result of a program and exit.  