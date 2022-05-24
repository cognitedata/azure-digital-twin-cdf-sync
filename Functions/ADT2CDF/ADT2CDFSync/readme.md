# ADT&rarr;CDF Sync

This is the event-triggered Azure function that processes create/update/delete ADT events and synchronizes any changes in the ADT&rarr;CDF direction.

The function pushes the following changes to CDF:

- asset creation (if it does not exist yet in CDF),

- asset property changes including metadata,

- asset parent change (when multiple parents are added in ADT, an error is displayed),

- asset deletion (if it does exist in CDF),

- new relationship creation (with or without labels), 

- relationship label updates (add, remove, replace),

- relationship delete,

- timeseries creation (if it does not exist yet in CDF),

- timeseries property changes including metadata, 

- timeseries linked asset change (when multiple assets are added in ADT, an error is displayed), 

- timeseries delete (if it does exist in CDF),

- latest datapoint change in timeseries (both the lates value and its timestamp must be provided, otherwise nothing is inserted in CDF).


## How `EventHubTrigger` works

Use the function trigger to respond to an event sent to an event hub event stream. You must have read access to the underlying event hub to set up the trigger. Event Hub Trigger gets fired in case of any events are delivered to the Azure event hub. 

In our case, ADT needs to be set up to route events to the event hub.
