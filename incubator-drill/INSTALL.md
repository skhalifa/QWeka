# How to build and run Apache Drill

## Prerequisites

Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.  To build, you need to have the following software installed on your system to successfully complete a build. 
  * Java 7
  * Maven 3.x

## Confirm settings
    # java -version
    java version "1.7.0_09"
    Java(TM) SE Runtime Environment (build 1.7.0_09-b05)
    Java HotSpot(TM) 64-Bit Server VM (build 23.5-b02, mixed mode)
    
    # mvn --version
    Apache Maven 3.0.3 (r1075438; 2011-02-28 09:31:09-0800)

## Checkout

    git clone https://github.com/apache/incubator-drill.git
    
## Build

    cd incubator-drill
    mvn clean install
    or
    mvn clean install -DskipTests

## Explode tarball in installation directory
   
    mkdir /opt/drill
    tar xvzf distribution/target/*.tar.gz --strip=1 -C /opt/drill 

## Use web interface @ http://localhost:8047/


## Start SQLLine (which starts Drill in embedded mode)
    Install and run zookeeper (https://support.lucidworks.com/hc/en-us/articles/203187153-Install-and-start-Zookeeper-server-on-Windows)
    cd /opt/drill
    bin/!quit
	or
	bin/sqlline -u "jdbc:drill:schema=dfs.root;zk=local" -n admin -p admin
	
	Linux:  ./sqlline -u jdbc:drill:zk=local
	
## Run a query

	SELECT employee_id, first_name FROM cp.`employee.json`; 

	SELECT max(cast(HIRE_DATE as date)) as MAX_DATE, min(cast(HIRE_DATE as date)) as MIN_DATE FROM `employee.json`;

	select R_REGIONKEY, cast(R_NAME as varchar(15)) as region, cast(R_COMMENT as varchar(255)) as comment from dfs.`C:\Users\shadi\workspace\incubator-drill\distribution\target\apache-drill-0.9-SNAPSHOT\apache-drill-0.9-SNAPSHOT\sample-data\region.parquet`;
    
    SELECT myaddints(CAST(position_id AS int),CAST(store_id AS int)) FROM cp.`employee.json`;
    
## More information 

For more information including how to run a Apache Drill cluster, visit the [Apache Drill Wiki](https://cwiki.apache.org/confluence/display/DRILL/Apache+Drill+Wiki)

