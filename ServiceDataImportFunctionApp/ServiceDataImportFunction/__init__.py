import logging
import os
import azure.functions as func

from .service_data_import.ptv_importer import *

def main(TestTrigger: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if TestTrigger.past_due:
        logging.info('The timer is past due!')

    logging.info("Running service data importer {}".format(utc_timestamp))
    service_data_importer = PTVImporter()
    service_data_importer.import_services()
    utc_timestamp2 = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()
    logging.info("Finished running service data importer {}".format(utc_timestamp2))

    logging.info('Python timer trigger function ran at %s', utc_timestamp2)
