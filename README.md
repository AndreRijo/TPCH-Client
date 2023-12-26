# PotionDB's TPC-H Client

A client for PotionDB that implements [TPC-H's](https://www.tpc.org/tpch/) benchmark.
It is assumed the reader is somewhat familiar with [TPC-H's specification](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).

For Eurosys reviewers, please use the Docker image provided at `andrerj/tpchclient:vldb`.
If building from source is a must, consider changing to the *vldb* branch: the main branch will remain stable (code-wise) during the review process, but may be updated later on.

## Features

- Full support of TPC-H's dataset: all entries of all tables are fully converted to PotionDB's objects;
- Full support of dataset updates, as defined in the [TPC-H's specification](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp);
- Efficient support of the following TPC-H queries: 3, 5, 11, 14, 16 and 18;
- Materialized and automatically maintained views for the queries above, with high throughput and sub-millisecond reply time;
- Automatic loading of TPC-H's dataset and required views into PotionDB;
- Highly customizable client: many different options are supported to fully evaluate PotionDB in multiple different settings.
- And more :)

## Initial setup

Before the client can be used, it is necessary to prepare TPC-H's dataset.
This can be summarized into the following tasks:

1. Download [TPC-H tools](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) and generate the dataset;
2. (Optionally) Modify the dataset to give locality to the data (For the motivation and explanation on this, please check [here])(https://github.com/AndreRijo/TPCH-LocalityTool).
3. Preparing PotionDB's setup: executing 5 instances of PotionDB (one for each inhabited continent in the world) and configuring partial replication.

Note: For Eurosys reviewers, step 2 is obligatory in order to reproduce the results reported in the paper.

### TPC-H tool and dataset generation

First, download the TPC-H tools [here](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
This will be necessary to generate the TPC-H dataset.

Afterwards, you will need to build the tool.
Refer to the README file included with the tool for instructions, as well as the [TPC-H specification](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).

For the parameters of the makefile, use the following:

```
DATABASE=ORACLE
WORKLOAD=TPCH
```

To generate the dataset and the set of updates to be used, run, respectively:

```
dbgen -s 1
dbgen -s 1 -u 1000
```

Note: If testing PotionDB on a single machine with modest hardware, you may want to test with a lower `-s` (for example, 0.1).
While the tool works for values under 1, said values are not officially supported by TPC-H.

Finally, organize the dataset and updates using the following folder structure:

```
tpch_data\
    tables\$sSF\
    upds\$sSF\
    headers\
```

Where `$s`corresponds to the value used for `-s` when generating the dataset.
For example, for `-s 1`:

```
tpch_data\
    tables\1SF\
    upds\1SF\
    headers\
```

Put all generated tables and updates, respectively, under `tables\$sSF` and `upds\$sSF`.
Fill the `headers` folder with the contents of the [tpch_headers folder](./tpch_headers) in this repository. 

### Modifying the dataset for locality

Use the TPC-H Locality tool that we have made, available here: https://github.com/AndreRijo/TPCH-LocalityTool.

After the new dataset and updates are generated by the TPC-H Locality tool, replace the contents of the folders *tables* and *upds* with the new data.
The new data will be inside a folder named *mod* that is inside both *tables* and *upds*.

### Preparing PotionDB for TPC-H

Please check the instructions in [PotionDB's github page](https://github.com/AndreRijo/potionDB/tree/remoteOp) on how to start PotionDB.
It is recommended to read its full README before proceding further with PotionDB's TPC-H client.

[This section in particular](https://github.com/AndreRijo/potionDB/tree/remoteOp#Running-multiple-instances-of-PotionDB) explains how to run 5 instances of PotionDB that are ready for TPC-H benchmarking.
Make sure to start those 5 instances before attempting to start the client and wait until a message similar to this one shows up in every instance:

```
PotionDB started at port 8087 with ReplicaID 23781
```

Make sure to use the Docker image provided at andrerj/potiondb (for Eurosys reviewers, andrerj/potiondb:vldb).
If for some reason you do not want to do so, please check on [PotionDB's github page](https://github.com/AndreRijo/potionDB/tree/remoteOp) for other options which may fit you.

## PotionDB's TPC-H Client setup

#### Option 1: Using docker and pre-compiled image (easiest solution)

Start by installing Docker: https://docs.docker.com/engine/install/

To obtain the pre-compiled Docker image of PotionDB's TPC-H Client, do:

```
docker pull andrerj/tpchclient
```

#### Option 2: Using Docker and compile from source

First, prepare a folder to hold all the required files.
We'll refer to such folder as the *baseFolder*.

Afterwards, go inside the *baseFolder* and clone [PotionDB's](https://github.com/AndreRijo/potionDB/tree/remoteOp) repository and this one:

```
git clone https://github.com/AndreRijo/potionDB.git potionDB
git clone https://github.com/AndreRijo/TPCH-Client.git tpch_client
```

Make sure to checkout PotionDB into the branch *remoteOp*.
For this, go inside the folder *potionDB* and run:

```
git checkout remoteOp
```

If not already done, install Docker. For reference: https://docs.docker.com/engine/install/

Afterwards, from the *baseFolder*, build the Docker image:

```
docker build -f tpch_client/Dockerfile . -t tpchclient
```

In the [Executing the client with Docker](#Executing-the-client-with-Docker) section, for all commands replace `andrerj/tpchclient` with `tpchclient`.

#### Option 3: Using Go and running from source, without Docker

Proceed as in [Option 2](#Option-2:-Using-Docker-and-compile-from-source) up to (exclusive) the instalation of Docker.

Make sure you have Go installed:

```
go version
```

If not, check https://go.dev/doc/install on how to install Go.

Now, inside *baseFolder*/tpch_client/src, run:

```
go run ./main/tpchMain.go --data_folder=$path_to_data --config=$path_to_config --test_name=$path_to-results
```

Where:

- `$path_to_data` is the path to the folder *tpch_data*
- `$path_to_config` is the path to the folder with the configuration file intended to be used;
- `path_to_results` is the path to the folder where you want to store the results of the benchmarking.

In the three parameters above, both relative and full paths are acceptable.

For details on how to configure the client, please check [Customizing the client](#Customizing-the-client).


## Executing the client with Docker

(For executing the client without Docker, please check the previous [subsection](#Option-3:-Using-Go-and-running-from-source,-without-Docker
))

Start the client with the following:

```
docker run -it --name tpch1 -v "$your_path_to_data/tpch_data/:/go/data/" -v "$your_path_to_configs/:/go/configs/extern/" -v "your_path_to_results/:/go/results/" -e CONFIG="/go/configs/extern/$YOUR_CONFIG_FOLDER/" andrerj/tpchclient
```

Where:

- `$your_path_to_data` is the full path to the folder *tpch_data* created earlier;
- `your_path_to_configs`is the path to the folder with your own/modified configuration files. If using one of the default ones, this `-v` can be removed and you should set `CONFIG=/go/configs/path_to_the_choosen_config/`;
- `your_config_folder` the relative (using as base $your_path_to_configs) path to the folder with the intended configuration file. For example, if your config.cfg is at folder `/home/yourname/myconfigs/mytpch`, `$your_path_to_data` would be `home/yourname/myconfigs` and `your_config_folder`would be `mytpch`;
- `your_path_to_results` the full path to the folder where you want to store the statistics of executing the TPC-H benchmark.

Note: Depending on your operating system and Docker installation, you may need to set up your Docker to allow sharing of folders.

An example of usage with a custom configuration file:

```
docker run -it --name tpch1 -v "home/myname/tpch_data:/go/data/" -v "$home/myname/myTpchClientConfigs/:/go/configs/extern/" -v "home/myname/tpch_results/:/go/results/" -e CONFIG="/go/configs/extern/myconfig/" andrerj/tpchclient
```

Or with a pre-made configuration file:
```
docker run -it --name tpch1 -v "home/myname/tpch_data:/go/data/" -v "home/myname/tpch_results/:/go/results/" -e CONFIG="/go/configs/singlePC/0.1SF/" andrerj/tpchclient
```

You can use the command above if you're testing PotionDB in your local machine. 
You can also try swapping 0.1SF with 1SF for a more intensive test.
If the servers are being deployed on multiple machines, you will likely want to make your own configuration file based on [configs/cluster/1SF/config.cfg](configs/cluster/1SF/config.cfg).

The client will start by loading the database into PotionDB.
After the data is fully loaded, a pre-defined number of clients will start querying the server.
Please check [Customizing the client](#Customizing-the-client) on how to configure the client's options.

## Customizing the client

A full list of all options for configuration files is available on [src/client/tpchClient.go](src/client/tpchClient.go), method *loadConfigsFile()*.
It is also possible to set most of those settings by using Docker's environment variables: check [src/client/tpchClient.go](src/client/tpchClient.go), method *loadFlags()* and [dockerstuff/start.sh](dockerstuff/start.sh)

Always use one of the existing configuration files as a base: the configurations in [configs/cluster](configs/cluster) and [configs/singlePC](configs/singlePC) are good starting points.
You will likely want to change the settings named `servers`, `queryClients`, `queryWait`, `nReadsTxn` and `updRate`.

Here's some of the most relevant, available settings:

- `multiServer (true/false)`: if data is spread across multiple servers or all in one;
- `globalIndex (true/false)`: the intended usage of potionDB, that is, to have views reflect data from all servers. Set this to false to simulate views with only local (and thus, incomplete for most queries) data;
- `singleIndexServer (true/false)`: by default false, in order for all servers to replicate all views. Can be set to true to simulate a single, central server responsible for processing all views;
- `servers`: list of servers (IP:port), separated by a space. The order *matters*: define them according to the order of which regions are spread across servers (check the `buckets` setting of each server - the one with `R1` is the first server, the one with `R2` the second, etc). For example, if executing locally, this would be: `potiondb1:8087, potiondb2:8087, potiondb3:8087, potiondb4:8087, potiondb5:8087`. Ports will be usually `:8087` unless PotionDB is being deployed without Docker or all servers are in the same machine but the client is in another machine;
- `scale`: corresponds to the value used for `-s` when the dataset was generated with `dbgen`;
- `doDataLoad (true/false)`: defines if the client should do the initial loading of the database or not. If it is intended to run multiple instances of the client in different machines, this should be set to false in all clients bar one;
- `doQueries` and `doUpdates (true/false on both)`: defines if the client should do queries or updates. Set them both to false if the intention is for the client to only do the initial loading of the database - otherwise, keep them both on true and control the update ratio with `updRate`;
- `updRate (0-1)`: defines the percentage of operations that should be updates. Setting to 0 or 1 corresponds to only doing, respectively, queries or updates;
- `startUpdFile` and `finishUpdFile`: if running multiple instances of the client, you will need to adjust this to ensure each instance has a non-overlaping subset of update files;
- `queryClients`: number of connections to use to execute queries and/or updates. You will want to vary this in order to grasp how PotionDB scales;
- `queryWait (ms)`: time for the clients to wait before starting to execute the TPC-H benchmark. The client will subtract from the waiting time any time it took preparing itself and/or doing the initial loading of the database.
This is useful for coordinating multiple instances of the client. Recommended to be set to twice of the time it takes to load the database.
This time will, unavoidably, depend on your system and `scale` value: try first running a single client instance only to measure the time of loading the database with its initial state.
- `queryDuration (ms)`: the time for which TPC-H's benchmark is run for, after `queryWait` ends; 
- `id`: the name of the client. When running multiple client instances, it is important to set a different `id` for each client, in order to avoid conflicts when statistics files are produced in a shared folder environment.
- `statsLocation`: the location to save the statistics files produced by the client.
- `statisticsInterval (ms)`: time between which statistics are recorded. For most use cases it is fine to leave this unchanged (5000ms).
- `tpchAddRate (0-1)`: when executing updates, defines the percentage of updates that are new orders, compared to deletion of orders. TPC-H's specification suggests to set this to 0.5, however in a real-world scenario for an e-commerce application this is heavily skewed towards 1 (mostly new with very rare deletions).
- `nReadsTxn`: number of operations (either read or updates) to execute per transaction. A higher value may lead to a higher throughput, at the sacrifice of possibly higher latency per transaction.
