import azure.functions as func
from typing import List
from .handler import handle


def main(events: List[func.EventHubEvent]):
    #for event in events:
    #    logging.info('Python EventHub trigger processed an event: %s',
    #                    event.get_body().decode('utf-8'))
    #    #logging.info("CloudEvent subject: %s", event.metadata["PropertiesArray"][0]["cloudEvents:subject"])
    #    logging.info("CloudEvent metadata: %s", event.metadata["PropertiesArray"])

    handle(events)    
    return