# Day to day activities for a Data Engineer with the AI agents

As introduced in the [methodology chapter](https://jbcodeforce.github.io/flink-studies/methodology/data_eng_101/), the data engineers have a lot to address during the life of a data streaming proceesing project. This chapter maps those activities to the tools of this repository and the agent skill to potentially use.

This chapter is structured by use cases, and the other descriptions in this documentation may go deeper in the command references.

The acronymes used are:

| Acronym/Term | Definition |
| ---- | ----- |
| **DSP** | Data Streaming Project. Groups Kafka, schema registry, Flink processing and Tableflow | 

## Create a DSP Project

* Create a project directory:
    ```sh
    mkdir $HOME/dsp-project
    cd $HOME/dsp-project
    git init
    ```
* Start your harness CLI or IDE (IBM Bob). If you use an IDE, load the project in your workspace.
* Execute a prompt in your agent harness or run a command:

    | Agent prompt | using /dbt-project create a dbt project under current folder.  use data as a product structure |
    | -----| ---- |
    | **Command** | `uv run sl-dbt init $HOME/dsp-project --type data-product --profile cc_flink`  | 

## Define / update user's .dbt/profiles.yml

## Manage Flink elements

### Add data product

Data product will help to group Flink pipelines related to the same appplication, same analytics metrics. 

* Execute a prompt in your agent harness or run a command:

    | Agent prompt | add a data analytics product named crm in this project |
    | -----| ---- |
    | **Command** | `uv run sl-dbt sl-dbt add-data-product .$HOME/dsp-project crm  | 

### Add table to a data product

| Agent prompt | add a table to deduplicate raw customer in sources, name it: src_customers for the crm data product |
| -----| ---- |
| **Command** | `uv run sl-dbt sl-dbt  add-table .$HOME/dsp-project crm --table-type source |

### Create raw topics

To prepare dbt run, the sources is referencing exising kafka topics. Sometime, for demonstration purpose, or the topic is not yet available in the kafka cluster. 

| Agent prompt | add a raw topic named raw_customers in the project as raws |
| -----| ---- |
| **Command** | `uv run sl-dbt add-raw-topic $HOME/dsp-project raw_customers`|

Then deploy the raw table

| Agent prompt | add a raw topic named raw_customers in the project as raws |
| -----| ---- |
| **Command** | `uv run flink-sql-deploy --sql-dir $HOME/dsp-project/pipelines/raws deploy` |

