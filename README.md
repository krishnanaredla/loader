# Data Loader 

 Pyspark based framework which picks up data processed by pre-processor, loads CSV files from landing buckets adds audit/partition columns and writes data into s3 based delta lake.

 * Data loader generates schema from meta db table
 * Can handle schema evolution, taking advantage of delta merge schema option
 * Framework can process multiple files at once (in parallel)
 

  Limitations: 

    * Currently it supports only one primary key
    * Supports only one defining column (Column/s which tells about new data)

## Config 

TypeSafe config to handle the configurations,
below is the sample conf file 

``` python
dev
{
  spark 
  {
      config
      {
          "spark.sql.extension" = "io.delta.sql.DeltaSparkSessionExtension"
      }
  }

  Audit 
  {
      db = postgresDB

  }
}

```

## Steps to run 

> Framework currently takes in two parameters , config file location through --config or -c and region in which framework is running through --region or -r,defaulting to dev

```console  
--config /location/to/application.conf --region prod
```

### Running locally 

Assuming access to s3 and postgres and enabled 

```console
pip install -r requirements.txt

spark-submit --jars delta***.jar dloader.py --config app.conf 
```

### Docker Image

> In-progress

### Kubernetes

Spark Operator will be used to run on k8s


## Testing 

pytest is used to run unit tests

> pytest.ini file is configured to include coverage and reporting details

run below command for unit tests
```console 
pip install -r requirements_test.txt

pytest 
```

### Docker Image

> In-progress