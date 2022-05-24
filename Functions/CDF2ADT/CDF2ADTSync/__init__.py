import os
import datetime
import logging

import azure.functions as func
from .handler import handle


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    EXTERNAL_ID = os.environ['ROOT_ASSET_EXTERNAL_ID']
    handle(EXTERNAL_ID)



