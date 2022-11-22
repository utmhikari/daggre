# daggre

**DAta-AGGREgator**, a tool to handle data aggregation tasks

Inspired by [mongodb aggregation](https://www.mongodb.com/docs/manual/aggregation/), daggre allows you to specify data aggregation pipelines via json-based configurations, so you need not to code anymore. A single aggregation pipeline can be applied to multiple groups of data, no matter where the data comes from, so that the pipelines are easily to be managed.

## Scenarios

A typical scenario is for config-table-check in game QA works. In game development, config-tables may appear as not only DB data but also code lines, made into files of different forms. 

When testing new features, such as malls and markets, the QA may need a joint-view of tables of malls, categories, products and items, linked by different primary-key ids, to check if the actual config fits the demand. And also, for some game systems like main/side-quests, the QA may need some fixed rules to monitor and check if the config data have no potential bug risks.

For joint-views of multiple tables, an aggregation pipeline with **lookup** and **unwind** stage can be able to join and list all the data. For example, if you have these tables:

```python
malls = [
    {"id": 1, "name": "Weapon", "products": [1, 2, 3]},
    {"id": 2, "name": "Armor", "products": [4]}
]

products = [
    {"id": 1, "name": "AWP", "itemID": 45},
    {"id": 2, "name": "AK47", "itemID": 42},
    {"id": 3, "name": "M4A1", "itemID": 43},
    {"id": 4, "name": "Kevlar": "itemID": 51}
]
```

Pick **malls** as main table, after **unwind** the **products** and **lookup** the **product** by **id**, we are able to get the final joint-view:

```python
malls_products = [
    {"id": 1, "name": "Weapon", "product": {"id": 1, "name": "AWP", "itemID": 45}},
    {"id": 1, "name": "Weapon", "product": {"id": 2, "name": "AK47", "itemID": 42}},
    {"id": 1, "name": "Weapon", "product": {"id": 3, "name": "M4A1", "itemID": 43}},
    {"id": 2, "name": "Armor", "product": {"id": 4, "name": "Kevlar": "itemID": 51}}
]
```

For data-check works, pipelines with **filter** stages is needed. For example, if we have items data like these:

```python
items = [
    {"id": 1, "name": "Molotov", "desc": "fire in the hole"},
    {"id": 2, "name": "Flashbang", "desc": "hooxi sucks"}
]
```

If we don't want to see the dirty words, just **filter** the rows by the rule: **desc** excludes **suck**.

## Usage

### daggre-cli

**DAGGRE-CLI** is the command-line program for processing data & aggregation specifications.

Compile `daggre_cli.go` to generate the executable, arguments are follows:

- `-h`: show help message
- `--workdir`: working directory for daggre-cli
- `--datapath`: the input json file path of data source, relative to `workdir`
- `--aggrepath`: the input json file path of aggregation specification, relative to `workdir`
- `--outputpath`: the output json file path of aggregated data, relative to `workdir`
- `--statspath`: the output json file path of aggregation statistics, relative to `workdir`

For the input data of `datapath`, the contents should be dict of table names and row data:

```json
{
  "tableName": [
    {"rowID": 1, "name": "foo"},
    {"rowID": 2, "name": "bar"}
  ],
  "tableName2": [
    {"id": 1, "name":  "hello"}
  ]
}
```

For the input aggregation of `aggrepath`, the contents should specify all pipelines and a main pipeline, which looks like this:

```json
{
  "pipelines": [
    {
      "name": "pipeline1",
      "desc": "my first pipeline",
      "tables": ["tableName"],
      "stages": [
        {
          "name": "filter",
          "params": {
            "locator": "rowID",
            "operator": "<=",
            "value": 1
          }
        }
      ]
    },
    {
      "name": "pipeline2",
      "desc": "my second pipeline",
      "tables": ["tableName2"],
      "stages": []
    }
  ],
  "main": "pipeline1"
}
```

Each pipeline should have:

- unique **name**
- optional **desc**
- init **tables**, which would be merged when the pipeline start
- **stages** to be processed

For each stage, the stage **name** and **params** should be specified, see **Pipeline Stages** below for details.

Finally, **main** pipeline must be specified, as the basic data for final output.

After execution of **daggre-cli**, the json output files are generated at `outputpath` and `statspath`.

The file of `outputpath` contains the final aggregated data, while the file of `statspath` contains the stats of all executed pipelines, these are:

- if the aggregation runs successfully
- error message
- input & output data sizes
- start & end unix timestamps
- status, output size, start & end unix timestamps of all the pipeline stages

See `res/testcases/cli` for examples of workplaces and results.

### daggre-svr

**DAGGRE-SVR** is an HTTP server offering aggregation services, based on [gin](https://github.com/gin-gonic)

Compile `daggre-svr.go` to generate the executable, arguments are follows:

- `-h`: show help message
- `-c`: specify yaml config file path

The yaml config file should specify the listen port:

```yaml
port: 8954
```

After launching the server, users should **POST** **json body** to `/api/v1/aggre` to start aggregation tasks.

The **json body** should be like this:

```json
{
  "data": "the dataset, same as the contents of 'datapath' in daggre-cli",
  "aggre": "the aggregation, same as the contents of 'aggrepath' in daggre-cli"
}
```

And the server would respond **json body** like this:

```json
{
  "output": "the final output data, same as the contents of 'outputpath' in daggre-cli",
  "stats": "the aggregation statistics, same as the contents of 'statspath' in daggre-cli"
}
```

Launch server and test `res/testcaes/svr/aggre_test.go` to take a try!

## Pipeline Stages

### filter

**FILTER** stage can filter the rows based on the rules specified, the params are follows:

- `locator`: a string to locate the value key by key, with dot `.` as the separator
- `operator`: comparison operators only: `<`, `<=`, `>`, `>=`, `==`, `!=` 
- `value`: the value to be compared

### lookup

**LOOKUP** stage can join two tables based on specific columns, the params are follows:

- `fromPipeline`: rows from which pipeline to joint into
- `localLocator`: the locator specifies the local joint-key
- `foreignLocator`: the locator specifies the foreign joint-key
- `toField`: which field the foreigh row data to joint info

### sort

**SORT** stage can sort the current table in place, the params are follows:

- `rules`: sort rules in priority order
  - `locator`: the locator to locate the row value
  - `order`: the sort order, should be either `1` (ASC) or `-1` (DESC)

### unwind

**UNWIND** stage can flatten an array into multiple items linking to same copies of row data.

The params are follows:

- `locator`: the locator to locate the array value
- `includeArrayIndex`: if not empty, specifies the key to hold the array index value
- `preserveNullAndEmptyArrays`: whether preserve the row if array value cannot be located

## Customization

You are able to customize your own pipeline stages by doing these steps:

- declare the `struct` your stage, which should contain `daggre.BasePipelineStage` and your stage params as members
- implement methods of `daggre.PipelineStageInterface` if necessary
  - `Check`: check if there is error in stage params
  - `ChildPipelines`: declare all the stage params representing other pipelines
  - `Process`: pipeline process logic
- implement factory function `NewXXXStage`, then call `daggre.RegisterPipelineStage` to register it

see `res/testcases/zzz/custom/custom_stage_test.go` for an example
