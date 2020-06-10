# mongodb-ey-pi

## Installation

Install

* git
* python3
* dotnet core

sudo pip3 install pymongo dnspython pysys

## Preparation

``` 
    cd testcases

    cp unix.properties.tmpl unix.properties
```

Edit `unix.properties` to point to

* Data dir for raw files
* location of mongodb tools
* location of dotnet core
* MongoDB Atlas Connection Strings
* Which connection string to use for the test [MONGODB_M30, MONGODB_M40, COSMOS (in MDB API compatibility mode  )]

## Running the tests

The tests use a Python test framework called [Pysys](https://github.com/pysys-test/pysys-test), which is framework designed to assist in creating system level test. Each pysys test is in a directory with test code (```run.py```) and directories for input and output.

To run a test, open a command shell and navigate to the testcases folder. In that folder there are separate folders for the MongoDB and Cosmos tests. To run the MongoDB ingestion test, cd to ```testcases\mongodb\import``` folder and type

```pysys run import_all```

This will run the automated python test to import all the CSV data files into MongoDB.

The results of the test are saved back into the MongoDB database in a collection called ```test_results```.


