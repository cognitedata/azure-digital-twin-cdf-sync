"""
EventHub triggered Azure function, to synchronize knowledge graph in the ADT->CDF direction.
"""
import os
import json
import logging
import time
from enum import Enum
from typing import List, Tuple, Union
from datetime import datetime, timezone

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials

from cognite.client.data_classes import Asset, AssetUpdate, LabelDefinition, Relationship, RelationshipUpdate, TimeSeries, TimeSeriesUpdate


# constant definitions
ROOT_EXTERNAL_ID = os.environ['ROOT_ASSET_EXTERNAL_ID']

class ADT_MODEL_IDS(Enum):
    ASSET = 'dtmi:digitaltwins:cognite:cdf:Asset;1'
    TIMESERIES = 'dtmi:digitaltwins:cognite:cdf:TimeSeries;1'

class CLOUD_EVENT_TYPES(Enum):
    TWIN_CREATE = 'Microsoft.DigitalTwins.Twin.Create'
    TWIN_UPDATE = 'Microsoft.DigitalTwins.Twin.Update'
    TWIN_DELETE = 'Microsoft.DigitalTwins.Twin.Delete'
    RELATIONSHIP_CREATE = 'Microsoft.DigitalTwins.Relationship.Create'
    RELATIONSHIP_UPDATE = 'Microsoft.DigitalTwins.Relationship.Update'
    RELATIONSHIP_DELETE = 'Microsoft.DigitalTwins.Relationship.Delete'

# class definitions
class EventRepresentation(object):
    type: str
    resource: str
    subject: str
    body: dict

class MetadataConversion(object):
    value: str
    key: str

# disable HTTP request and response logs (headers etc.)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)


def handle(events: List[func.EventHubEvent]):
    '''
    Parses cloud events from the Event Hub, and handles the event by pushing the appropriate changes to CDF.
    '''
    #logging.info('Event list length: %s', str(len(events)))
    # update CDF asset hierarchy according to ADT changes
    cdf_client = get_cdf_client()
    adt_client = get_adt_client()    
    rootAsset = cdf_client.assets.retrieve(external_id=ROOT_EXTERNAL_ID)
    if not rootAsset:
        logging.error('Error: root asset with external id: "%s" was not found in CDF!', ROOT_EXTERNAL_ID)
        return
    eventIndex = 0
    for event in events:
        metadata = event.metadata["PropertiesArray"]
        if (len(metadata) != len(events)):
            logging.error('Length of cloud event list (%s) does not match length of metadata (%s)!', len(events), len(metadata))
            return
        event_representation = parse_event(event, metadata[eventIndex])
        if not(event_representation.type in ['Create', 'Update', 'Delete']):
            logging.error('Error: unknown event type "%s" for event with body %s', event_representation.type, event_representation.body)
            return
        logging.info('EVENT INFO: %s %s "%s" at %s.', event_representation.type, event_representation.resource, event_representation.subject, metadata[eventIndex]['cloudEvents:time'])
        
        result = False
        if (event_representation.resource == 'asset'):
            result = handle_asset(cdf_client, adt_client, event_representation)
        elif (event_representation.resource == 'relationship'):
            result = handle_relationship(cdf_client, adt_client, event_representation)
        elif (event_representation.resource == 'timeseries'):
            result = handle_timeseries(cdf_client, adt_client, event_representation)
        else:
            logging.error('Unknown cloud event type "%s" for event: %s', event_representation.resource, event_representation.body)

        if (result):
            logging.info('Event processed sucesfully for subject "%s"!', event_representation.subject)
        else:
            logging.info('There was a problem when processing event for subject "%s"! Check log messages above for errors/warnings!', event_representation.subject)
        
        eventIndex = eventIndex + 1
    return eventIndex


def handle_asset(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, event_representation: EventRepresentation) -> bool:
    '''
    Performs operation on asset.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param EventRepresentation event_representation: representation of cloud event
    :return bool: True if operation on asset was successfully performed
    '''
    event_body = event_representation.body
    adt_id = event_representation.subject
    if (('externalId' in event_body) and (event_body['externalId'])):
        adt_id = event_body['externalId']

    if event_representation.type == "Create":
        return create_asset(cdf_client, adt_id, event_body)
    elif event_representation.type == "Update":
        return update_asset(cdf_client, adt_client, adt_id, event_body)
    elif event_representation.type == "Delete":
        return delete_asset(cdf_client, adt_id)


def handle_relationship(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, event_representation: EventRepresentation) -> bool:
    '''
    Performs operation on relationship.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param EventRepresentation event_representation: representation of cloud event
    :return bool: True if operation on relationship was successfully performed
    '''
    if (event_representation.type in ['Create', 'Delete']):   # check in advance if source or target exists for 'Create' or 'Delete'
        rel_endptspoints = get_rel_endpoints(cdf_client, adt_client, event_representation.body)
        if not(rel_endptspoints):
            return False
        else:
            rel_source = rel_endptspoints[0]
            rel_target = rel_endptspoints[1]
    
    if (event_representation.type == 'Create'):
        return create_relationship(cdf_client, adt_client, event_representation.body, rel_source, rel_target)
    elif (event_representation.type == 'Update'):
        return update_relationship(cdf_client, event_representation.body, event_representation.subject)
    elif (event_representation.type == 'Delete'):
        return delete_relationship(cdf_client, adt_client, event_representation.body, rel_source, rel_target)


def handle_timeseries(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, event_representation: EventRepresentation) -> bool:
    '''
    Performs operation on timeseries.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param EventRepresentation event_representation: representation of cloud event
    :return bool: True if operation on timeseries was successfully performed
    '''
    event_body = event_representation.body
    adt_id = event_representation.subject
    if (('externalId' in event_body) and (event_body['externalId'])):
        adt_id = event_body['externalId']

    if (event_representation.type == 'Create'):
        return create_timeseries(cdf_client, adt_id, event_body)
    elif (event_representation.type == 'Update'):
       return update_timeseries(cdf_client, adt_client, adt_id, event_body)
    elif (event_representation.type == 'Delete'):
        return delete_timeseries(cdf_client, adt_id)


###############################################################################
############################## utility functions ##############################
###############################################################################
def parse_event(event: func.EventHubEvent, event_properties: dict) -> EventRepresentation:
    '''
    Parses the cloud event from the EventHub and converts it to 'EventRepresentation' for easier usage.
    :param func.EventHubEvent event: the cloud event object
    :param dict event_properties: additional information on event from metadata
    :return EventRepresentation: The polished representation of the cloud event
    '''
    try:
        try:
            cloud_event_type = CLOUD_EVENT_TYPES(event_properties["cloudEvents:type"])
            event_representation = EventRepresentation()
            event_representation.body = json.loads(event.get_body().decode('utf-8'))
            event_representation.subject = event_properties['cloudEvents:subject']
            cloud_event_parts = cloud_event_type.value.split('.')
            if (len(cloud_event_parts) >= 4 and cloud_event_parts[3] in ['Create', 'Update', 'Delete']):
                event_representation.type = cloud_event_parts[3]
            if (cloud_event_parts[2] == 'Twin'):
                if (cloud_event_parts[3] == 'Create' or cloud_event_parts[3] == 'Delete'):
                    model = event_representation.body['$metadata']['$model']
                elif (cloud_event_parts[3] == 'Update'):
                    model = event_representation.body['modelId']
                else:
                    model = None
                if (model == ADT_MODEL_IDS.ASSET.value):
                    event_representation.resource = 'asset'
                elif (model == ADT_MODEL_IDS.TIMESERIES.value):
                    event_representation.resource = 'timeseries'
            elif (cloud_event_parts[2] == 'Relationship'):
                event_representation.resource = 'relationship'
            return event_representation
        except ValueError:
            logging.error('Cloud event type "%s" is not handled by the current solution!', event_properties["cloudEvents:type"])

    except KeyError:
        logging.error('Key error when parsing cloud event with body: %s and metadata: %s!', event.get_body().decode('utf-8'), event_properties)


def create_asset(cdf_client: CogniteClient, adt_id: str, event_body: dict) -> bool:
    '''
    Creates a new asset in CDF, if it does not exist yet.
    :param CogniteClient cdf_client: The CDF client object
    :param str adt_id: The ID of the asset from ADT
    :param dict event_body: body of cloud event holding data about the new asset
    :return bool: True if asset was successfully created
    '''
    if not has_asset_in_CDF_by_external_id(cdf_client, adt_id):
        # create the new asset
        new_asset = Asset(external_id=adt_id, parent_external_id=ROOT_EXTERNAL_ID)
        if ('displayName' in event_body):
            new_asset.name = event_body['displayName']
        else:
            new_asset.name = event_body.get('$dtId')    # name is mandatory in CDF
        if ('description' in event_body):
            new_asset.description = event_body['description']
        if ('values' in event_body['tags']):
            new_asset.metadata = event_body['tags']['values']
        cdf_client.assets.create(new_asset)
    else:
        logging.warning('CDF asset with external ID "%s" already exists!', adt_id)
        return False
    return True


def update_asset(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, adt_id: str, event_body: dict) -> bool:
    '''
    Updates an existing asset in CDF.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param str adt_id: The ID of the asset from ADT
    :param dict event_body: body of cloud event holding data about the asset
    :return bool: True if asset was successfully updated
    '''
    asset_to_update = cdf_client.assets.retrieve(external_id=adt_id)
    if not(asset_to_update):  # maybe ID contains special characters
        dt = adt_client.get_digital_twin(adt_id)
        if ('externalId' not in dt):
            logging.error('Cannot perform update! CDF asset with external ID "%s" does not exist!', adt_id)
            return False
        asset_to_update = cdf_client.assets.retrieve(external_id=dt['externalId'])
        if not(asset_to_update):
            logging.error('Cannot perform update! CDF asset with external ID "%s" does not exist!', dt['externalId'])
            return False
    try:
        has_change, asset_to_update = fetch_changes_to_CDF_record(cdf_client, asset_to_update, event_body)
        if has_change:
            cdf_client.assets.update(asset_to_update)
        else:
            logging.warning('Nothing to update! The asset "%s" is already up to date!', asset_to_update.external_id)
        return True
    except KeyError as e:
        logging.error('KeyError in update_asset %s !', e)
        return False


def delete_asset(cdf_client: CogniteClient, adt_id: str) -> bool:
    '''
    Deletes an existing asset from CDF.
    :param CogniteClient cdf_client: The CDF client object
    :param str adt_id: The ID of the asset from ADT
    :return bool: True if asset was successfully deleted
    '''
    if has_asset_in_CDF_by_external_id(cdf_client, adt_id):
        cdf_client.assets.delete(external_id=adt_id)
    else:
        logging.warning('CDF asset with external ID "%s" was not found!', adt_id)
    return True


def has_asset_in_CDF_by_external_id(cdf_client: CogniteClient, adt_id: str) -> bool:
    '''
    Check if asset exists in CDF using cloud event subject as an external ID
    :param CogniteClient cdf_client: The CDF client object
    :param str adt_id: The ID of the asset from ADT
    :return bool: True if asset exists
    '''
    asset = cdf_client.assets.retrieve(external_id=adt_id)
    # logging.info('asset: %s %s', asset, (asset is not None))
    return (asset is not None)


def fetch_changes_to_CDF_record(cdf_client: CogniteClient, record_to_update: Union[Asset, TimeSeries], event_body: dict):
    '''
    Updates the given CDF resource in memory, according to changes originating from the event body.
    :param CogniteClient cdf_client: The CDF client object
    :param Asset|TimeSeries record_to_update: The current CDF resource to be updated
    :param dict event_body: body of cloud event holding data about the new resource
    :return (bool, Asset|TimeSeries): True if changes were made, and the updated resource
    '''
    has_change = False
    converted_metadata = convert_metadata(record_to_update.metadata)
    for action in event_body['patch']:
        # check if the was updated before to close the event loop
        if action['path']=='/displayName':
            if action['op']=='remove':
                continue
            else:
                if record_to_update.name!=action['value'] :
                    record_to_update.name = action['value']
                    has_change = True
        elif (action['path'] == '/description'):
            if (action['op'] == 'remove'):
                if (record_to_update.description):
                    record_to_update.description = ''
                    has_change = True
            else:
                if (record_to_update.description != action['value']):
                    record_to_update.description = action['value']
                    has_change = True
        elif action['path']=='/externalId':
            # the external_id is immutable in CDF
            if action['op']=='add':
                # check if the new external ID exists in CDF
                if not has_asset_in_CDF_by_external_id(cdf_client, adt_id=action['value']):
                    logging.error('Inconsistent hierarchy warning. The external ID "%s" already exists in CDF!', action['value'])
            else:
                logging.error('Invalid operation: DO NOT modify the "externalId" property in ADT!')
            continue
        elif action['path']=='/id':
            if action['op']!='add':
                logging.error('Invalid operation: DO NOT modify the "id" property in ADT!')
            continue
        elif (action['path'] == '/tags/values'):
            if (action['op'] == 'add'):
                if not(converted_metadata):
                    record_to_update.metadata = action['value']
                    has_change = True
                else:
                    logging.warning('Trying to add metadata, but it already exists in CDF!')
            if (action['op'] == 'remove'):
                if (converted_metadata):
                    record_to_update.metadata = {}
                    has_change = True
        else:
            # fetch metadata
            if action['path'].startswith('/tags/values/'):
                path = action['path'].replace('/tags/values/', '')
                if action['op']=='replace':
                    has_change, record_to_update = check_value_change_and_update_record(has_change, record_to_update, converted_metadata, path, action['value'])

                if action['op']=='add':
                    has_change, record_to_update = check_value_change_and_update_record(has_change, record_to_update, converted_metadata, path, action['value'])

                if action['op']=='remove' and path in converted_metadata:
                    has_change = True
                    del record_to_update.metadata[converted_metadata[path].key]

    return has_change, record_to_update


def check_value_change_and_update_record(has_change: bool, record_to_update: Union[Asset, TimeSeries], 
    converted_metadata: dict, metadata_key: str, metadata_value: str):
    '''
    Updates the metadata for the CDF resource in memory, according to a given key and value pair.
    :param bool has_change: Signifies if there were any changes before
    :param Asset|TimeSeries record_to_update: The current CDF resource to be updated
    :param dict converted_metadata: CDF metadata converted to be able to handle special keys as well
    :param str metadata_key: Metadata key to check if it exists yet
    :param str metadata_value: Metadata value to check if it needs to be updated
    :return (bool, Asset|TimeSeries): True if changes were made, and the updated resource
    '''
    if metadata_key in converted_metadata:
        if converted_metadata[metadata_key].value != metadata_value:
            has_change = True
            record_to_update.metadata[converted_metadata[metadata_key].key] = metadata_value
    else:
        # add metadata if not found in CDF
        has_change = True
        record_to_update.metadata[metadata_key] = metadata_value

    return has_change, record_to_update


def get_rel_endpoints(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, rel: dict) -> Union[Tuple[Asset, Union[Asset, TimeSeries]], None]:
    '''
    Retrieves the CDF source and target resource of an ADT relationship.
    The external ID of the relationship endpoints might have been converted (because of special characters),
    so the "externalId" field of the source/target digital twin needs to be checked too in this case.
    The source is always an Asset, while the target can be Asset or TimeSeries.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param dict rel: The relationship structure from ADT
    :return Tuple: tuple of source and target resources
    '''
    # source is always Asset (at least in the current solution)
    source_asset = cdf_client.assets.retrieve(external_id=rel['$sourceId'])
    # target can be Timeseries or Asset
    if (rel['$relationshipName'] == 'contains'):
        target_res = cdf_client.time_series.retrieve(external_id=rel['$targetId'])
    else:
        target_res = cdf_client.assets.retrieve(external_id=rel['$targetId'])
    if ((source_asset) and (target_res)):
        return (source_asset, target_res)

    if not(source_asset):
        # maybe external ID has special characters, check original external ID too
        try:
            source_dt = adt_client.get_digital_twin(digital_twin_id=rel['$sourceId'])
        except ResourceNotFoundError:
            source_dt = {}
        if ('externalId' not in source_dt):
            logging.warning('The asset with digital twin ID "%s" does not exist in ADT anymore!', rel['$sourceId'])
            return None
        source_asset = cdf_client.assets.retrieve(external_id=source_dt['externalId'])
        if not(source_asset):
            logging.warning('The asset with external ID "%s" does not exist in CDF!', source_dt['externalId'])
            return None

    if (rel['$relationshipName'] == 'contains'):
        if not(target_res):
            # maybe external ID has special characters
            try:
                target_dt = adt_client.get_digital_twin(digital_twin_id=rel['$targetId'])
            except ResourceNotFoundError:
                target_dt = {}
            if ('externalId' not in target_dt):
                logging.warning('The timeseries with digital twin ID "%s" does not exist in ADT anymore!', rel['$targetId'])
                return None
            target_res = cdf_client.time_series.retrieve(external_id=target_dt['externalId'])
            if not(target_res):
                logging.warning('The timeseries with external ID "%s" does not exist in CDF!', target_dt['externalId'])
                return None
    else:
        if not(target_res):
            # maybe external ID has special characters
            try:
                target_dt = adt_client.get_digital_twin(digital_twin_id=rel['$targetId'])
            except ResourceNotFoundError:
                target_dt = {}
            if ('externalId' not in target_dt):
                logging.error('The timeseries with external ID "%s" does not exist in ADT anymore!', rel['$targetId'])
                return None
            target_res = cdf_client.assets.retrieve(external_id=target_dt['externalId'])
            if not(target_res):
                logging.error('The asset with external ID "%s" does not exist in CDF!', target_dt['externalId'])
                return None
    return (source_asset, target_res)


def create_relationship(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, event_body: dict,
    rel_source: Asset, rel_target: Union[Asset, TimeSeries]) -> bool:
    '''
    Creates new (implicit/explicit) relationship in CDF.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param dict event_body: body of the cloud event
    :param Asset rel_source: source asset of the relationship to be created
    :param Asset|TimeSeries rel_target: target asset/timeseries of the relationship to be created
    :return bool: True if relationship was successfully created
    '''
    ############################## parent relationship ##############################
    if (event_body['$relationshipName'] == 'parent'):  # parent relationship between assets
        # get other parent relationships and DO NOT change parent if this is another parent relationship
        parent_rels = adt_client.query_twins('SELECT R FROM DIGITALTWINS T JOIN CT RELATED T.parent R ' +
            'WHERE T.$dtId = \'' + event_body['$sourceId'] + '\'' +
            ' and R.$relationshipId != \'' + event_body['$relationshipId'] + '\'')
        if (next(parent_rels, None)):
            logging.error('An asset in CDF can have a single parent, but another parent was added to asset "%s". ' +
                'Aborting synchronization! Please delete the relationship with ID: "%s" from ADT!', event_body['$sourceId'], event_body['$relationshipId'])
            return False
        # update the parent in CDF
        a = cdf_client.assets.retrieve(external_id=rel_source.external_id)
        if (a.parent_external_id == rel_target.external_id):
            logging.warning('The parent of "%s" in CDF is already "%s"!', rel_source.external_id, rel_target.external_id)
            return True
        else:
            u = AssetUpdate(external_id=rel_source.external_id).parent_external_id.set(rel_target.external_id)
            cdf_client.assets.update(u)
            return True

    ############################## contains relationship ##############################
    elif (event_body['$relationshipName'] == 'contains'):  # timeseries is linked to an asset
        # get other 'contains' relationships and DO NOT change asset if this is a another relationship
        contain_rels = adt_client.query_twins('SELECT R FROM DIGITALTWINS T JOIN CT RELATED T.contains R ' +
            'WHERE CT.$dtId = \'' + event_body['$targetId'] + '\'' +
            ' and R.$relationshipId != \'' + event_body['$relationshipId'] + '\'')
        if (next(contain_rels, None)):
            logging.error('A timeseries in CDF can have a single asset linked, but another asset was added to timeseries "%s". ' +
                'Aborting synchronization! Please delete the relationship with ID: "%s" from ADT!', event_body['$targetId'], event_body['$relationshipId'])
            return False
        # update the linked asset in CDF
        ts = cdf_client.time_series.retrieve(external_id=rel_target.external_id)
        if (ts.asset_id == rel_source.id):
            logging.warning('The linked asset of "%s" in CDF is already "%s"!', rel_target.external_id, rel_source.external_id)
            return False
        else:
            u = TimeSeriesUpdate(external_id=rel_target.external_id).asset_id.set(rel_source.id)
            cdf_client.time_series.update(u)
            return True
    
    ############################## relatesTo relationship ##############################
    elif (event_body['$relationshipName'] == 'relatesTo'):     # real relationship between assets
        if (cdf_client.relationships.retrieve(event_body['$relationshipId'])):
            logging.warning('The CDF relationship with external ID "%s" already exists!', event_body['$relationshipId'])
            return False
        labels = []
        if ('labels' in event_body):
            labels = event_body['labels'].split(',')
        for l in labels:
            if not(cdf_client.labels.list(external_id_prefix=l)):
                logging.error('Cannot create CDF relationship. The label with the external ID "%s" does not exist!', l)
                return False
        rel = Relationship(external_id=event_body['$relationshipId'],
            source_external_id=rel_source.external_id, source_type='asset',
            target_external_id=rel_target.external_id, target_type='asset',
            labels=labels)
        cdf_client.relationships.create([rel])
        return True
    else:
        logging.error('The relationship with body: %s cannot be handled by the current solution!', event_body)
        return False


def update_relationship(cdf_client: CogniteClient, event_body: dict, event_subject: str) -> bool:
    '''
    Updates an explicit relationship in CDF (an ADT 'relatesTo' relationship): for now only the labels can be changed.
    :param CogniteClient cdf_client: The CDF client object
    :param dict event_body: body of the cloud event
    :param str event_subject: subject of the cloud event (contains the ADT relationship ID, which is the CDF external ID)
    :return bool: True if relationship was successfully updated
    '''
    if (('patch' not in event_body) or (event_body['patch'][0]['path'] != '/labels')):
        logging.error('The "%s" structure is not a proper relationship update!', event_body)
        return False
    subject_parts = event_subject.split('/')
    if (len(subject_parts) != 3 or subject_parts[1] != 'relationships'):
        logging.error('Cannot perform relationship update "%s". Subject "%s" does not point to a proper relationship!', event_body, event_subject)
        return False
    if (('value' in event_body['patch'][0]) and (event_body['patch'][0]['value'])):
        labels_new = event_body['patch'][0]['value'].split(',')
    else:
        labels_new = []
    for l in labels_new:
        label_definitions = cdf_client.labels.list(external_id_prefix=l)
        if (not(label_definitions) or (label_definitions[0].external_id != l)):
            logging.warning('The CDF relationship label with external ID "%s" does not exist, creating it now!', l)
            cdf_client.labels.create(LabelDefinition(external_id=l, name=l))
            break
    rel = cdf_client.relationships.retrieve(external_id=subject_parts[2])
    labels_old = list(map(lambda x: x['externalId'], rel.labels))
    labels_add = []
    for l in labels_new:
        if not(l in labels_old):
            labels_add.append(l)
    labels_remove = []
    for l in labels_old:
        if not(l in labels_new):
            labels_remove.append(l)
    if ((labels_add) or (labels_remove)):
        rel_update = RelationshipUpdate(external_id=subject_parts[2]).labels.add(labels_add).labels.remove(labels_remove)
        cdf_client.relationships.update(rel_update)
    else:
        logging.warning('Nothing to update! The labels of relationship "%s" are already up to date!', subject_parts[2])
    return True


def delete_relationship(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, event_body: dict,
    rel_source: Asset, rel_target: Union[Asset, TimeSeries]) -> bool:
    '''
    Deletes an existing (implicit/explicit) relationship in CDF.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param dict event_body: body of the cloud event
    :param Asset rel_source: source asset of the relationship to be deleted
    :param Asset|TimeSeries rel_target: target asset/timeseries of the relationship to be deleted
    :return bool: True if relationship was successfully deleted
    '''
    ############################## parent relationship ##############################
    if (event_body['$relationshipName'] == 'parent'):  # parent relationship between assets
        # check is parent is actually the same
        if (rel_source.parent_external_id != rel_target.external_id):
            logging.warning('Parent not deleted in CDF! You are deleting the "%s"->"%s" parent relationship from ADT, ' +
            'but the parent is actually "%s" in CDF!', event_body['$sourceId'], event_body['$targetId'], rel_source.parent_external_id)
            return True
        # get other parent relationships and DO NOT change parent if this is another relationship
        parent_rels = adt_client.query_twins('SELECT R FROM DIGITALTWINS T JOIN CT RELATED T.parent R ' +
            'WHERE T.$dtId = \'' + event_body['$sourceId'] + '\'' +
            ' and R.$relationshipId != \'' + event_body['$relationshipId'] + '\'')
        parent_rels = list(parent_rels)
        if (len(parent_rels) > 0):  # there are other parents in ADT
            logging.warning('Changing parent in CDF! You are deleting the "%s"->"%s" parent relationship from ADT, but ' +
            '"%s" is another parent in ADT, setting it in CDF as well!', event_body['$sourceId'], event_body['$targetId'], parent_rels[0]['R']['$targetId'])
            new_parent_asset = cdf_client.assets.retrieve(external_id=parent_rels[0]['R']['$targetId'])
            if not(new_parent_asset):
                dt = adt_client.get_digital_twin(parent_rels[0]['R']['$targetId'])
                new_parent_asset = cdf_client.assets.retrieve(external_id=dt['externalId'])
                if not(new_parent_asset):
                    logging.error('The asset with external ID "%d" does not exist in CDF!', dt['externalId'])
                    return False
            u = AssetUpdate(external_id=rel_source.external_id).parent_external_id.set(new_parent_asset.external_id)
            cdf_client.assets.update(u)
            return True
        else:
            u = AssetUpdate(external_id=rel_source.external_id).parent_external_id.set(ROOT_EXTERNAL_ID)
            cdf_client.assets.update(u)
            return True
    
    ############################## contains relationship ##############################
    elif (event_body['$relationshipName'] == 'contains'):  # timeseries is linked to an asset
        # check is linked asset is actually the same
        if (rel_source.id != rel_target.asset_id):
            logging.warning('Timeseries-asset link not deleted in CDF! You are deleting the "%s"->"%s" link relationship from ADT, ' +
            'but the linked asset is different in CDF!', event_body['$sourceId'], event_body['$targetId'])
            return True
        # get other contain relationships and DO NOT change asset if this is another
        contain_rels = adt_client.query_twins('SELECT R FROM DIGITALTWINS T JOIN CT RELATED T.contains R ' +
            'WHERE CT.$dtId = \'' + event_body['$targetId'] + '\'' +
            ' and R.$relationshipId != \'' + event_body['$relationshipId'] + '\'')
        contain_rels = list(contain_rels)
        if (len(contain_rels) > 0):  # there are other linked assets in ADT
            logging.warning('Changing linked asset in CDF! You are deleting the "%s"->"%s" link relationship from ADT, but ' +
            '"%s" is another linked asset in ADT, setting it in CDF as well!', event_body['$sourceId'], event_body['$targetId'], contain_rels[0]['R']['$sourceId'])
            new_linked_asset = cdf_client.assets.retrieve(external_id=contain_rels[0]['R']['$sourceId'])
            if not(new_linked_asset):
                dt = adt_client.get_digital_twin(contain_rels[0]['R']['$sourceId'])
                new_linked_asset = cdf_client.assets.retrieve(external_id=dt['externalId'])
                if not(new_linked_asset):
                    logging.error('The asset with external ID "%d" does not exist in CDF!', dt['externalId'])
                    return False
            u = TimeSeriesUpdate(external_id=rel_target.external_id).asset_id.set(new_linked_asset.id)
            cdf_client.time_series.update(u)
            return True
        else:
            root = cdf_client.assets.retrieve(external_id=ROOT_EXTERNAL_ID)
            u = TimeSeriesUpdate(external_id=rel_target.external_id).asset_id.set(root.id)
            cdf_client.time_series.update(u)
            return True

    ############################## relatesTo relationship ##############################
    elif (event_body['$relationshipName'] == 'relatesTo'):     # real relationship between assets
        rel = cdf_client.relationships.retrieve(external_id=event_body['$relationshipId'])
        if (rel):
            if (rel.source_external_id != event_body['$sourceId']):
                logging.warning('Relationship "%s" not deleted from CDF, because the source asset in CDF ("%s") is different from ADT ("%s")!', event_body['$relationshipId'], rel.source_external_id, event_body['$sourceId'])
                return True
            if (rel.target_external_id != event_body['$targetId']):
                logging.warning('Relationship "%s" not deleted from CDF, because the target asset in CDF ("%s") is different from ADT ("%s")!', event_body['$relationshipId'], rel.target_external_id, event_body['$targetId'])
                return True
            cdf_client.relationships.delete(external_id=event_body['$relationshipId'])
            return True
        else:
            logging.error('Relationship with external ID "%s" does not exist in CDF!', event_body['$relationshipId'])
            return False
    else:
        logging.error('The relationship with body: %s cannot be handled by the current solution!', event_body)
        return False


def check_and_insert_datapoint(cdf_client: CogniteClient, adt_id: str, latest_value: str, timestamp_str: str) -> bool:
    '''
    Add a new datapoint to a CDF timeseries, if the given timestamp if after the current latest value.
    :param CogniteClient cdf_client: The CDF client object
    :param string adt_id: Cloud event subject that detetmines the timeseries
    :param string latest_value: datapoint value
    :param string timestamp_str: datapoint datetime value as string
    :return bool: True if new datapoint has been added
    '''
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()
    datapoints = cdf_client.datapoints.retrieve_latest(external_id=adt_id)
    
    timeseries = cdf_client.time_series.retrieve(external_id=adt_id)
    
    datapoint = datapoints[0] if len(datapoints)>0 else None
    
    if (not(datapoint) or (datapoint.timestamp/1000 < timestamp)):
        latest_value_to_insert = latest_value
        try:
            latest_value_to_insert = float(latest_value)
            if (timeseries.is_string):
                logging.error('Cannot insert number "%s" into string timeseries "%s"!', latest_value_to_insert, timeseries.external_id)
                return False
        except ValueError:
            if not(datapoint):
                logging.warning('The value "%s" is the first one to be inserted into timeseries "%s". Converting timeseries to string BY RECREATION!', latest_value_to_insert, timeseries.external_id)
                # !!cannot update the "is_string" property of timeseries, have to delete and create again
                timeseries.is_string = True
                timeseries.id = None
                timeseries.created_time = None
                timeseries.last_updated_time = None
                cdf_client.time_series.delete(external_id=timeseries.external_id)
                time.sleep(0.1) # have to wait, otherwise 'create' returns duplicate error (delete in cloud is not fast enough)
                while (cdf_client.time_series.retrieve(external_id=timeseries.external_id)):
                    time.sleep(0.1)
                    pass    # delete operation is async, so make sure timeseries is not in the cloud anymore
                cdf_client.time_series.create(timeseries)
            else:
                if not(timeseries.is_string):
                    logging.error('Cannot insert string "%s" into numeric timeseries "%s"!', latest_value_to_insert, timeseries.external_id)
                    return False
        new_datapoint_value = {'timestamp': timestamp*1000, 'value': latest_value_to_insert}
        cdf_client.datapoints.insert([new_datapoint_value], external_id=timeseries.external_id)
        logging.info('New datapoint added with values %s to timeseries "%s"!', new_datapoint_value, adt_id)
        return True

    return False


def create_timeseries(cdf_client, adt_id, event_body) -> bool:
    '''
    Creates a new timeseries in CDF, if it does not exist yet.
    :param CogniteClient cdf_client: The CDF client object
    :param str adt_id: The ID of the timeseries from ADT
    :param dict event_body: body of cloud event holding data about the new timeseries
    :return bool: True if timeseries was successfully created
    '''
    if not has_timeseries_in_CDF_by_external_id(cdf_client, adt_id):
        # get root asset object        
        rootAsset = cdf_client.assets.retrieve(external_id=ROOT_EXTERNAL_ID)
        
        # create the new timeseries
        new_data = TimeSeries(name=adt_id, external_id=adt_id,  asset_id=rootAsset.id)
        if ('displayName' in event_body):
            new_data.name = event_body['displayName']
        else:
            new_data.name = event_body.get('$dtId')    # name is mandatory in CDF            
        if ('description' in event_body):
            new_data.description = event_body['description']
        if ('values' in event_body['tags']):
            new_data.metadata = event_body['tags']['values']

        cdf_client.time_series.create(new_data)
        logging.info('Timeseries "%s" was created!', adt_id)
        
        if ('latestValue' in event_body) and ('timestamp' in event_body) and (event_body['latestValue']) and (event_body['timestamp']):
            # check if latestValue and timestamp is exists in timeseries data
            check_and_insert_datapoint(cdf_client, adt_id, event_body['latestValue'], event_body['timestamp'])
    else:
        logging.warning('CDF timeseries "%s" already exists!', adt_id)
        return False
    return True


def update_timeseries(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, adt_id: str, event_body: dict) -> bool:
    '''
    Updates an existing timeseries in CDF.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param str adt_id: The ID of the timeseries from ADT
    :param dict event_body: body of cloud event holding data about the timeseries
    :return bool: True if timeseries was successfully updated, or a new datapoint was inserted into it
    '''
    timeseries_to_update = cdf_client.time_series.retrieve(external_id=adt_id)
    if not(timeseries_to_update):  # maybe ID contains special characters
        dt = adt_client.get_digital_twin(adt_id)
        if ('externalId' not in dt):
            logging.error('Cannot perform update! CDF timeseries with external ID "%s" does not exist!', adt_id)
            return False
        timeseries_to_update = cdf_client.time_series.retrieve(external_id=dt['externalId'])
        if not(timeseries_to_update):
            logging.error('Cannot perform update! CDF timeseries with external ID "%s" does not exist!', dt['externalId'])
            return False

    has_change, timeseries_to_update = fetch_changes_to_CDF_record(cdf_client, timeseries_to_update, event_body)

    # update timeseries record
    if has_change:
        cdf_client.time_series.update(timeseries_to_update)
        #logging.info("CDF time series has been updated with data: %s", timeseries_to_update)

    # update latest datapoint if needed
    has_datapoint = False
    for action in list(event_body['patch']):
        # check if the was updated before to close the event loop
        if action['path']=='/latestValue':
            if action['op']=='remove':
                continue
            else:
                action_pair = [ action_item for action_item in event_body['patch'] if '/timestamp' in action_item.get('path') ]
                if len(action_pair) > 0 and action_pair[0].get('op')!='remove':
                    # delete the pair change from the patch list
                    event_body['patch'].remove(action_pair[0])
                    latest_value  = action.get('value')
                    timestamp_str = action_pair[0].get('value')
                    has_datapoint = check_and_insert_datapoint(cdf_client, timeseries_to_update.external_id, latest_value, timestamp_str)
                else:
                    # if the value pair was removed or not found in the modification list we skip the process of this
                    continue
    if (not(has_change) and not(has_datapoint)):
        logging.warning('Nothing to update! Timeseries "%s" and latest value is already up to date!', timeseries_to_update.external_id)
        return True
    return (has_change or has_datapoint)


def delete_timeseries(cdf_client: CogniteClient, adt_id: str) -> bool:
    '''
    Deletes an existing timeseries from CDF.
    :param CogniteClient cdf_client: The CDF client object
    :param str adt_id: The ID of the timeseries from ADT
    :return bool: True if timeseries was successfully deleted
    '''
    if has_timeseries_in_CDF_by_external_id(cdf_client, adt_id):
        cdf_client.time_series.delete(id=None, external_id = adt_id)
    else:
        logging.warning('Timeseries "%s" was not found in CDF!', adt_id)
    return True


def has_timeseries_in_CDF_by_external_id(cdf_client: CogniteClient, adt_id: str) -> bool:
    '''
    Check if timeseries exists in CDF using cloud event subject as an external ID
    :param CogniteClient cdf_client: The CDF client object
    :param str adt_id: The ID of the asset from ADT
    :return bool: True if timeseries exists
    '''
    time_series = cdf_client.time_series.retrieve(external_id=adt_id)
    # logging.info('asset: %s %s', asset, (asset is not None))
    return (time_series is not None)


def convert_metadata(metadata: dict) -> dict:
    '''
    Converts CDF metadata to valid ADT format, replacing problematic characters in map keys:
        <space> =>  '_'
        '.'     =>  '^'
        '$'     =>  '#'
    WARNING: temporary solution
    '''
    new_map = {}
    for k in metadata:
        kk = k.replace(' ', '_').replace('.', '^').replace('$', '#')
        new_metadata = MetadataConversion()
        new_metadata.value = metadata[k]
        new_metadata.key = k
        new_map[kk] = new_metadata
    return new_map


def get_cdf_client() -> CogniteClient:
    """
    Retrieves a Cognite Data Fusion (CDF) client object, which allows to interact with CDF.
    Prerequisite: make sure the following environment variables are set
        CDF_TENANTID
        CDF_CLIENTID
        CDF_CLUSTER
        CDF_PROJECT
        CDF_CLIENT_SECRET
    :return: the CDF client object
    """
    TENANT_ID = os.environ["CDF_TENANTID"]
    CLIENT_ID = os.environ["CDF_CLIENTID"]
    CDF_CLUSTER =  os.environ["CDF_CLUSTER"]
    COGNITE_PROJECT =  os.environ["CDF_PROJECT"]

    SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]

    CLIENT_SECRET = os.environ["CDF_CLIENT_SECRET"]

    TOKEN_URL = "https://login.microsoftonline.com/%s/oauth2/v2.0/token" % TENANT_ID

    BASE_URL = f"https://{CDF_CLUSTER}.cognitedata.com"

    creds = OAuthClientCredentials(token_url=TOKEN_URL, client_id=CLIENT_ID, scopes=SCOPES, client_secret=CLIENT_SECRET)
    cnf = ClientConfig(client_name="cdf-optimisation", project=COGNITE_PROJECT, credentials=creds, base_url=BASE_URL)
    cdf_client = CogniteClient(cnf)

    #print(cdf_client.iam.token.inspect())
    return cdf_client


def get_adt_client() -> DigitalTwinsClient:
    """
    Retrieves an Azure Digital Twin (ADT) client object, which allows to interact with ADT.
    Prerequisite: make sure the "ADT_URL" environment variable is set.
        When running locally, the following variables are also needed:
            AZURE_SUBSCRIPTION_ID
            AZURE_TENANT_ID
            AZURE_CLIENT_ID
    :return: the ADT client object
    """
    url = os.environ["ADT_URL"]
    credential = DefaultAzureCredential()
    adt_client = DigitalTwinsClient(url, credential)

    return adt_client