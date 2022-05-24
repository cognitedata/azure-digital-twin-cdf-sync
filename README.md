# Cognite Data Fusion and Azure Digital Twin Plug-in

The purpose of the present plug-in is to synchronize the industrial knowledge graph between the Cognite Data Fusion (CDF) and Azure Digital Twin (ADT) platforms, using [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/) written in Python.

For development **_Python 3.9.7_** was used, as this was the latest version supported by Azure functions. For various ways of deployment, check [this link](https://docs.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies).

--------------------------------------------------------

## Ground Rules

The user must respect a few ground rules (guidelines) when making changes to the asset hierarchy, because of [issues and limitations](LIMITATIONS.md) that cannot be handled unambiguously by the current solution.

1. Do not change the external ID of resources in CDF. Instead, delete the resource first and create it again with the new external ID.

2. Do not use the same external ID for different type of resources in CDF.

3. Do not edit the “externalId” and “id” properties of resources (assets and timeseries for now) in ADT. Even if blank (i.e., not set) leave them as is, the CDF&rarr;ADT sync will take care of it.

4. In ADT do not create different relationships with the same ID, because in CDF they are unique.

5. For timeseries in ADT, update both the "latestValue" and the "timestamp" properties at the same time. Otherwise the new datapoint will not be inserted in CDF. Also, do not insert string values into numeric timeseries, and vice-versa.

--------------------------------------------------------

## Description

The solution translates a CDF asset hierarchy together with contextualized operational and engineering data into Digital Twin Definition Language (DTDL) ontologies, pushes the results to Azure, and synchronizes changes in the graph in both directions.

Currently, the following CDF resource types are mapped:
- Assets
- Asset-to-asset relationships
- Timeseries with the value of the latest datapoint

The project contains two main features:
1.	a timer-triggered Azure function to create/update the knowledge graph in the CDF&rarr;ADT direction,
2.	an event-triggered Azure function to update changes in the ADT&rarr;CDF direction.

The DTDL models used to represent resources in ADT are stored in the `Models` folder in this repository.

For more details about the Azure functions check the [CDF&rarr;ADT Readme](./Functions/CDF2ADT/CDF2ADTSync/readme.md) and [ADT&rarr;CDF Readme](./Functions/ADT2CDF/ADT2CDFSync/readme.md) files.

### Assets

CDF [Assets](https://cognite-docs.readthedocs-hosted.com/projects/cognite-sdk-python/en/latest/cognite.html#cognite.client.data_classes.assets.Asset) are translated into the `Asset` DTDL model together with all properties:

- CDF external ID and internal ID (remember not to edit these)
- name (which is mandatory in CDF)
- description
- metadata - represented by the `tags/values` map property in ADT

### Relationships

In the current solution only asset-to-asset CDF relationships are modeled, but at the same time two types of ADT relationships should be differentiated:

<ol>
<li>

Explicit relationships: in CDF they are the actual [Relationship](https://cognite-docs.readthedocs-hosted.com/projects/cognite-sdk-python/en/latest/cognite.html#cognite.client.data_classes.relationships.Relationship) resources and are represented by the <i>relatesTo</i> ADT relationship. IMPORTANT NOTE: these relationships can have multiple labels in CDF – check the [limitations](LIMITATIONS.md) on how this is handled.
</li>
<li>Implicit relationships: in CDF they are not separate resources but are stored as properties. In ADT they must still be represented as real relationships. These are the following (2 for now):
<ol type="a">
    <li>Parent-child relationship: stored in the “parent_external_id” field of a CDF asset, and represented in ADT by the <i>parent</i> relationship between Asset twins.</li>
    <li>Timeseries – belongs to – Asset relationship: stored in the “asset_id” field of a CDF Timeseries, and represented in ADT by the <i>contains</i> relationship between Asset and Timeseries twins.</li>
</ol>
</li>
</ol>

To summarize, currently there are 3 types of ADT relationships (_relatesTo, parent, contains_), all defined in the `Asset` model.

### Timeseries

CDF [Timeseries](https://cognite-docs.readthedocs-hosted.com/projects/cognite-sdk-python/en/latest/cognite.html#cognite.client.data_classes.time_series.TimeSeries) are translated into the `Timeseries` DTDL model and are similar to assets with the addition of 2 new properties holding the value and the timestamp, respectively, of the latest datapoint.

--------------------------------------------------------

## Dependencies

In order to deploy and run the plugin, the following resources are required:

- CDF tenant, which contains the initial industrial knowledge graph(s) to be mapped

- Microsoft Azure tenant, where the Azure functions will be deployed to replicate and synchronize the graph(s). The Azure resources below need to be created beforehand:
    - 2 function apps (one timer-triggered and one event-triggered),
    - 2 blob storage accounts (one for each function),
    - Key vault,
    - Azure Digital Twins,
    - Event Hub.

### Library Versions

The Python libraries used during the development of the two Azure functions are listed in the table below (last update on May 20, 2022).

<table>
<thead>
<tr>
    <th rowspan=2>Python Library</th>
    <th colspan=2 style="border:none;text-align:center">Version</th>
</tr>
<tr>
    <th>CDF&rarr;ADT</th>
    <th>ADT&rarr;CDF</th>
</tr>
</thead>
<tbody>
<tr>
    <td>azure-core</td>
    <td colspan=2 style="text-align:center">1.24.0</td>
</tr>
<tr>
    <td>azure-digitaltwins-core</td>
    <td colspan=2 style="text-align:center">1.1.0</td>
</tr>
<tr>
    <td>azure-eventhub</td>
    <td style="text-align:center">-</td>
    <td style="text-align:center">5.9.0</td>
</tr>
<tr>
    <td>azure-functions</td>
    <td colspan=2 style="text-align:center">1.11.2</td>
</tr>
<tr>
    <td>azure-identity</td>
    <td colspan=2 style="text-align:center">1.10.0</td>
</tr>
<tr>
    <td>azure-storage-blob</td>
    <td style="text-align:center">12.12.0</td>
    <td style="text-align:center">-</td>
</tr>
<tr>
    <td>cognite-sdk</td>
    <td colspan=2 style="text-align:center">2.49.1</td>
</tr>
</tbody>
</table>


### Environment Variables

Besides the knowledge graph itself, all the inputs for the functions must be defined as environment variables in the Azure function configuration settings. The table below summarizes the list of keys and the requirement for each function.

|Variable Key|Description|CDF&rarr;ADT|ADT&rarr;CDF|
|-|-|:-:|:-:|
|ADT_URL| URL of the ADT resource (with "https://")|YES|YES|
|<div style="max-width:200px">adtevents_RootManageSharedAccessKey_EVENTHUB|endpoint of the Event Hub|NO|YES
|AzureWebJobsStorage|connection string to the blob storage linked to this Azure function|YES|YES|
|CDF_CLIENT_SECRET|client secret of the Cognite tenant|YES|YES|
|CDF_CLIENTID|the client ID of the Cognite tenant|YES|YES|
|CDF_CLUSTER|cluster of the Cognite tenant|YES|YES|
|CDF_TENANTID|ID of the Cognite tenant|YES|YES|
|CDF_PROJECT|Cognite project inside the Cognite tenant|YES|YES|
|FUNCTIONS_EXTENSION_VERSION||"~4"|"~4"|
|FUNCTIONS_WORKER_RUNTIME|defaults to "python" in both cases|"python"|"python"|
|ROOT_ASSET_EXTERNAL_ID|the external ID of the root asset node of the knowledge graph to be instantiated and synchronized|YES|YES|

To run the Azure functions on your local computer, you may need to add additional environment variables in your `local.settings.json` file. Check [this documentation](https://docs.microsoft.com/en-us/azure/azure-functions/functions-develop-local) for more information.

--------------------------------------------------------

## Authors

Contributors names and contact info:

* Murad Sæter: murad.sater@cognitedata.com
* Janos Puskas: janos.puskas@accenture.com
* Robert-Adrian Rill: robert-adrian.rill@accenture.com
* Zsolt Tofalvi: zsolt.tofalvi@accenture.com
