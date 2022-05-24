# CDF&rarr;ADT Sync

This is the timer-triggered Azure function that replicates the CDF graph in ADT and synchronizes any changes in the CDF&rarr;ADT direction.

The function implements the following features:

- Mapping of CDF assets, asset-to-asset relationships and timeseries resources to their corresponding DTDL models (also uploaded to the Github repository) and instantiating the digital twins in ADT. This constitutes the replication of the CDF graph in ADT.

- Updating all the changes on these three CDF resources inside ADT: 

    - asset property changes including metadata,

    - asset parent change,

    - new asset creation,

    - old asset delete,

    - relationship property updates,

    - new relationship creation, 

    - old relationship delete,

    - timeseries property updates, 

    - timeseries linked asset change, 

    - new timeseries creation, 

    - old timeseries delete,

    - latest datapoint change in timeseries.


## How `TimerTrigger` works

The `TimerTrigger` makes it incredibly easy to have your functions executed on a schedule. This sample demonstrates a simple use case of calling your function every 5 minutes.

For a `TimerTrigger` to work, you provide a schedule in the form of a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression)(See the link for full details). A cron expression is a string with 6 separate expressions which represent a given schedule via patterns. The pattern we use to represent every 5 minutes is `0 */5 * * * *`. This, in plain text, means: "When seconds is equal to 0, minutes is divisible by 5, for any hour, day of the month, month, day of the week, or year".
