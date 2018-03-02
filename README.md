
# Taxi Demo - Nautilus Streaming Analytics Demo

## Overview

This projects demonstrates how to use several key features of Nautilus to perform real-time analytics
and visualization on streaming Internet-Of-Things (IOT) data.

- Data Preparation: The [notebooks](notebooks) directory contains a Jupyter notebook used to prepare the data that
  the Streaming Data Generator will use.
  This reads the NYC Yellow Taxi trip data and produces a set of JSON files containing events based on the trips.

- Streaming Data Generator: To allow this demo to be used without a NeuroSky MindWave Sensor, a streaming data generator has been
  created that can playback previously recorded data or synthetic data and send it to the RESTful gateway
  in the same way as the Effective Learner Application.
  This is a Python script and is in the [streaming_data_generator](streaming_data_generator) directory.
  
- Gateway: This is a RESTful web service that receives JSON documents from the Streaming Data Generator.
  It parses the JSON, adds the remote IP address, and then writes it to a Pravega stream.
  This is a Java application and is in the [gateway](gateway) directory.
  
- Pravega: Pravega provides a new storage abstraction - a stream - for continuous and unbounded data. 
  A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.
  See <http://pravega.io> for more information.
  
- Flink: Apache Flink® is an open-source stream processing framework for distributed, high-performing, always-available, and accurate data streaming applications.
  See <https://flink.apache.org> for more information.
  
- Flink Streaming Job: This is a Java application that defines a job that can be executed in the Flink cluster.
  This particular job simply reads the events from Prvega and loads them into Elasticsearch for visualization.
  This is in the [flinkprocessor](flinkprocessor) directory. 
  
- Elasticsearch: This stores the output of the Flink Streaming Job for visualization.

- Kibana: This provides visualization of the data in Elasticsearch.

- Docker: This demo uses Docker and Docker Compose to greatly simplify the deployment of the various
  components on Linux and/or Windows servers, desktops, or even laptops.
  For more information, see <https://en.wikipedia.org/wiki/Docker_(software)>.


## Building and Running the Demo

### Install Operating System

Install Ubuntu 16.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### Install Java

```
apt-get install default-jdk
```

### Install IntelliJ

Install from <https://www.jetbrains.com/idea>.
Enable the Lombok plugin. 
Enable Annotations (settings -> build, execution, deployment, -> compiler -> annotation processors). 

### Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### Data Preparation

- `cd notebooks`

- Visit <http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml> and download the
  March 2015 Yellow Taxi trip data.
  `wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-03.csv`
  
- Launch the Jupyter environment in Docker.
  `docker-compose up -d`
  
- Open your browser to Jupyter.
  <http://localhost:8888/notebooks/work/preprocess_data.ipynb>

- Open the `preprocess_data.ipynb` notebook.

- Execute all cell by clicking Cells -> Run All.
  This may take several minutes to run.
  It will write several files within a directory named data.json.
  These files will be used by the Streaming Data Generator.

### Install Pravega

This section can be skipped if you are using Nautilus.

- Refer to <http://pravega.io/docs/latest/deployment/run-local/>.
  Standalone Mode From Docker Container is recommended for a demo.  
  
### Run Gateway

- Open this directory in IntelliJ.

- Set the following environment variables for running the gateway project:
  - PRAVEGA_CONTROLLER: URI for the Pravega controller. For example: tcp://127.0.0.1:9091

- Execute the Gateway application.

### Run Streaming Data Generator

- You must use Anaconda Python 3.
  Below assumes that this is installed in ~/anaconda3.
  
- `~/anaconda3/bin/python streaming_data_generator/streaming_data_generator.py notebooks/data.json/*.json`

### Flink Job

- The Flink job can be executed with Nautilus, another Flink cluster, or in standalone mode.  
  In standalone mode, a mini Flink cluster will execute within the application process.
  When run in the IntelliJ IDE, standalone mode will be used by default.

- Use the following parameters, replacing `pravega_controller_ip` and `elastic_ip`.

  ```--controller tcp://pravega_controller_ip:9091 --elastic-sink true --elastic-host elastic_ip```


# References

- <http://pravega.io/>
- <https://flink.apache.org>
- <https://cwiki.apache.org/confluence/display/FLINK/Streams+and+Operations+on+Streams>
- <https://jersey.java.net/documentation/latest/getting-started.html>
- <http://www.oracle.com/webfolder/technetwork/tutorials/obe/java/griz_jersey_intro/Grizzly-Jersey-Intro.html>
