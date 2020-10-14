# PermID-Lance

Genearating source and target graph based on the PermID Project with the help of Lance.

## Device information

- OS: Ubuntu 16.04 LTS amd64
- Mem: 96G
- Disk: 20T

## Pre-request

### Python

1. Install anaconda via offical [tutorial](https://docs.anaconda.com/anaconda/install/linux/).
2. Create conda environment.

~~~bash
conda create --name kgpy35 python=3.5
...
...
conda activate kgpy35
~~~

3. Install request modules.

~~~bash
(kgpy35) pip install numpy scipy
~~~

### Java

1. Install Java 8
2. Config ENV_VARIABLE

## PermID

Data has more potential value when it can be shared or opened. It can be used by a number of different stakeholders and partners, within or outside an organization, for a variety of applications to gain new analytical insight and to build new products and services. And in order to effectively use data, it’s important to understand how it connects to the real world. That’s why Refinitiv is making available its Permanent Identifiers, or PermIDs, and the associated entity masters and metadata to the market. PermIDs are open, permanent and universal identifiers where underlying attributes capture the context of the identity they each represent.

See the [official web](https://permid.org/) for details. Due to the license limitation, please register and download the data from the official website by yourselves.

## Lance

Lance is a generic benchmark generator for linked data. It is the first benchmark generator for Linked Data to support semantics-aware test cases that take into account complex OWL constructs in addition to the standard test cases related to structure and value transformations. Lance supports the definition of matching tasks with varying degrees of difficulty and produces a weighted gold standard, which allows a more fine-grained analysis of the performance of instance matching tools. It can accept as input any linked dataset and its accompanying schema to produce a target dataset implementing test cases of varying levels of difficulty.

Please refer to the [paper](http://ceur-ws.org/Vol-1700/paper-03.pdf) and [project](https://github.com/jsaveta/Lance) for details

## Code

Please download and unzip the data into `data` dir under the same level of these codes. And run the following codes with pyspark.

- `gen_5k.py`: Extract the knowledge contained in the PermID project of approximate 5000 exchange listed companies in the United States to form the source knowledge graph. The extracted knowledge includes company name, headquarters address, official website URL, and management personnel. 
- `gen_20k.py`: Extract the knowledge contained in the PermID project of approximate 20k exchange listed companies , involving various aspects of knowledge of listed companies in different countries, including company name, country, headquarters address, official website URL, and management personnel.
- `gen_600k.py`: Extract the knowledge contained in the PermID project of approximate 600k exchange listed companies , involving various aspects of knowledge of listed companies in different countries, including company name, country, headquarters address, official website URL, and management personnel.

## Use Of Lance

The parameters used by LANCE is in **FTRLIM: Distributed Instance Matching Framework\\ for Large-Scale Knowledge Graphs**



