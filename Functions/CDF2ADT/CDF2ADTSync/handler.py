"""
Timer-triggered Azure function, to synchronize knowledge graph in the CDF->ADT direction.
"""
import os
import logging
import json
from enum import Enum
from datetime import datetime, timezone
from typing import Tuple, Union

from azure.core.exceptions import ResourceNotFoundError
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient, StorageStreamDownloader
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset, AssetList, TimeSeries


# constant definitions
class ADT_MODEL_IDS(Enum):
    ASSET = 'dtmi:digitaltwins:cognite:cdf:Asset;1'
    TIMESERIES = 'dtmi:digitaltwins:cognite:cdf:TimeSeries;1'

BLOB_CONTAINER_NAME = 'params'
BLOB_FILE_NAME = 'func_runs.json'   # this file will contain the timestamp of the last synchronization for each root asset

SQL_IN_BATCH_SIZE = 100
SQL_PLACEHOLDER = '<_:_>'

# disable HTTP request and response logs (headers etc.)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)


def handle(root_ext_id: str) -> None:
    # transform CDF asset hierarchy to ADT
    # Step 1: preparations
    cdf_client = get_cdf_client()
    adt_client = get_adt_client()
    root_asset = cdf_client.assets.retrieve(external_id=root_ext_id)
    if (root_asset is None):
        logging.error('The root asset with external ID "%s" does not exist in CDF. Aborting synchronization!', root_ext_id)
        return 

    # Step 2: create/update digital twin in ADT, based on any modifications in CDF
    # get the timestamp of the last synchronization
    sync_ts = get_last_exec_TS(root_ext_id)
    twin_exists = True
    try:
        adt_client.get_digital_twin(digital_twin_id=convert_ext_id(root_ext_id))
    except:
        twin_exists = False

    # consistency check
    if (sync_ts) and not(twin_exists):
        logging.error('Last CDF->ADT synchronization timestamp is defined, but the digital twin in ADT corresponding to the root asset "%s" from CDF does NOT exist!', root_ext_id)
        return
    if not(sync_ts) and (twin_exists):
        logging.warning('Last CDF->ADT synchronization timestamp is NOT defined, but the digital twin in ADT corresponding to the root asset "%s" from CDF does exist!', root_ext_id)
        #return

    asset_list = cdf_client.assets.retrieve_subtree(external_id=root_ext_id)
    date_now = datetime.utcnow()
    if not(sync_ts):
        logging.info('Creating new ADT digital twins for CDF asset hierarchy with root asset external ID "%s"!', root_ext_id)
        na = insert_assets(adt_client, asset_list)
        nr = insert_asset_to_asset_relationships(cdf_client, adt_client, asset_list)
        nt = insert_timeseries(cdf_client, adt_client, asset_list)
        logging.info('Created %d assets, %d relationships, %d timeseries in ADT!', na, nr, nt)
    else:
        logging.info('Updating existing ADT digital twins for CDF asset hierarchy with root asset external ID "%s"!', root_ext_id)
        ##asset_list = cdf_client.assets.list(root_external_ids=[root_ext_id], 
        ##    last_updated_time={'min': int(sync_ts*1000)})
        na = update_assets(adt_client, list(filter(lambda x: x.last_updated_time > sync_ts*1000, asset_list)))
        nr = update_asset_to_asset_relationships(cdf_client, adt_client, asset_list, sync_ts)
        nt = update_timeseries(cdf_client, adt_client, asset_list, sync_ts)
        nad = delete_assets(adt_client, asset_list[1:], root_ext_id)    # skip root node
        nrd = delete_asset_to_asset_relationships(cdf_client, adt_client, asset_list)
        ntd = delete_timeseries(cdf_client, adt_client, asset_list)
        logging.info('Updated %d assets, %d relationships, %d timeseries in ADT!', na, nr, nt)
        logging.info('Deleted %d assets, %d relationships, %d timeseries in ADT!', nad, nrd, ntd)
    set_last_exec_TS(root_ext_id, date_now.timestamp())
    logging.info('Finished synchronizing CDF asset hierarchy to ADT!')
    return


def insert_assets(adt_client: DigitalTwinsClient, asset_list: AssetList) -> int:
    '''
    Inserts the list of CDF assets into ADT as digital twins.
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets
    :return int: number of assets inserted
    '''
    n = 0
    for a in asset_list:
        ##print(a.external_id)
        create_twin(a, adt_client)
        n = n+1
    return n


def insert_asset_to_asset_relationships(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, asset_list: AssetList) -> int:
    '''
    Retrieves the list of CDF relationships from asset to asset, and inserts them into ADT.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets
    :return int: number of relationships inserted
    '''
    n = 0
    asset_external_ids = list(map(lambda x: x.external_id, asset_list))
    rel_list = cdf_client.relationships.list(
        source_external_ids=asset_external_ids, 
        target_external_ids=asset_external_ids, 
        limit=-1)
    for rel in rel_list:
        n = n+1
        labels = ','.join(list(map(lambda x: x['externalId'], rel.labels)))   # e.g. result: 'contains,flowsTo'
        insert_adt_relationship(adt_client, convert_ext_id(rel.source_external_id), convert_ext_id(rel.target_external_id), 
            'relatesTo', rel.external_id, labels)
    return n


def insert_timeseries(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, asset_list: AssetList) -> int:
    '''
    Retrieves all the timeseries from CDF linked to each asset from the given list, 
    and inserts them into ADT as digital twins.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets linked to the timeseries
    :return int: number of timeseries inserted
    '''
    n = 0
    # for each asset get the linked timeseries
    for a in asset_list:
        asset_ext_id = convert_ext_id(a.external_id)
        ts_list = cdf_client.time_series.list(asset_external_ids=[a.external_id], limit=-1)
        if not(ts_list):
            continue
        # create the corresponding digital twins for each timeseries
        for t in ts_list:
            n = n+1
            ext_id = convert_ext_id(t.external_id)
            langString = t.description if t.description else ''
            ts = cdf_client.datapoints.retrieve_latest(external_id=t.external_id)
            if (ts):
                d = ts[0]
            else:
                d = None
            ##print(asset_ext_id, len(ts_list), d.value, d.timestamp)
            temp_twin = get_twin_dict(t, ADT_MODEL_IDS.TIMESERIES)
            if (d):
                temp_twin['latestValue'] = str(d.value)
                temp_twin['timestamp'] = datetime.fromtimestamp(d.timestamp/1000, tz=timezone.utc)
            adt_client.upsert_digital_twin(ext_id, temp_twin)
            insert_adt_relationship(adt_client, asset_ext_id, ext_id, 'contains')
    return n


def update_assets(adt_client: DigitalTwinsClient, asset_list: AssetList) -> int:
    '''
    Compares each CDF asset from the given list with digital twins from ADT, and
    updates the changes or creates a new twin if it does not exist now.
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets
    :return int: number of assets updated
    '''
    n = 0
    for a in asset_list:
        ext_id = convert_ext_id(a.external_id)
        parent_ext_id = convert_ext_id(a.parent_external_id)
        
        try:
            dt = adt_client.get_digital_twin(digital_twin_id=ext_id)
            update_patches = get_update_patches(a, dt)
            if (update_patches):
                n = n+1
                adt_client.update_digital_twin(ext_id, update_patches)
            
            # verify parent change
            if not(parent_ext_id):  # this is the root asset
                continue
            rel_list_adt = adt_client.list_relationships(ext_id)
            rela = next((r for r in rel_list_adt if ((r['$sourceId'] == ext_id) and (r['$relationshipName'] == 'parent'))), None)
            if not(rela):   # relationship does not exist in ADT
                logging.warning('Skipping parent update for asset "%s", because it is not provided in ADT yet!', ext_id)
                continue
            if (rela['$targetId'] != parent_ext_id):   # parent changed, have to recreate the relationship
                if (rela['$relationshipId'] != rela['$sourceId'] + '->' + rela['$targetId']):
                    logging.warning('The parent relationship with ID "%s", between the "%s" and "%s" twins was likely not created' + 
                    'by CDF->ADT sync, and the ADT->CDF sync might have failed. Still, updating parent to "%s" now!', 
                    rela['$relationshipId'], rela['$sourceId'], rela['$targetId'], parent_ext_id)
                if not(update_patches):  # twin was not updated, but parent relationship was, so count this occurrence
                    n = n+1
                adt_client.delete_relationship(ext_id, rela['$relationshipId'])
                insert_adt_relationship(adt_client, ext_id, parent_ext_id, 'parent')
        except ResourceNotFoundError:
            n = n+1
            # this is a new digital twin
            create_twin(a, adt_client)
            # WARNING: external ID might have changed in CDF, this is not handled for now
    return n


def update_asset_to_asset_relationships(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, asset_list: AssetList, sync_ts: float) -> int:
    '''
    Compares CDF asset-to-asset relationships with digital twin relationships from ADT, and
    updates the changes, or recreates the relationship if source or target was modified.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets to check relationships between
    :param float sync_ts: timestamp of the last synchronization
    :return int: number of relationships updated
    '''
    n = 0
    asset_ext_ids = list(map(lambda x: x.external_id, asset_list))
    rel_list_cdf = cdf_client.relationships.list(
        source_external_ids=asset_ext_ids, 
        target_external_ids=asset_ext_ids, 
        last_updated_time={'min': int(sync_ts*1000)},
        limit=-1)
    if not(rel_list_cdf):
        return 0

    # retrieve all corresponding relationships from ADT
    sql_query = 'SELECT R FROM DIGITALTWINS T JOIN CT RELATED T.relatesTo R WHERE T.$dtId in ' + SQL_PLACEHOLDER
    rel_list_adt = query_adt_batches(adt_client, sql_query, list(map(lambda x: convert_ext_id(x), asset_ext_ids)))

    for rel in rel_list_cdf:
        source_ext_id = convert_ext_id(rel.source_external_id)
        target_ext_id = convert_ext_id(rel.target_external_id)
        labels = ','.join(list(map(lambda x: x['externalId'], rel.labels)))   # e.g. result: 'contains,flowsTo'
        rela = next((r['R'] for r in rel_list_adt if r['R']['$relationshipId'] == rel.external_id), None)
        if (not(rela) or target_ext_id != rela['$targetId'] or source_ext_id != rela['$sourceId']):
            # if-case: the relationship needs to be recreated in ADT if target/source changed or it does not exist yet at all
            n = n+1
            if (rela):
                # delete this existing relationship first
                adt_client.delete_relationship(rela['$sourceId'], rela['$relationshipId'])
            # now create new relationship
            insert_adt_relationship(adt_client, source_ext_id, target_ext_id, 'relatesTo', rel.external_id, labels)
        else:
            # else-case: the relationship may need to be updated
            update_patches = []
            if (labels):
                if not('labels') in rela:
                    update_patches.append({'op': 'add', 'path': '/labels', 'value': labels})
                elif (labels != rela['labels']):
                    update_patches.append({'op': 'replace', 'path': '/labels', 'value': labels})
            else:
                if ('labels' in rela):
                    update_patches.append({'op': 'remove', 'path': '/labels'})
            if (update_patches):
                n = n+1
                adt_client.update_relationship(rel.source_external_id, rel.external_id, update_patches)
    return n


def update_timeseries(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, asset_list: AssetList, sync_ts: float) -> int:
    '''
    Compares CDF timeseries for the given assets with digital twin timeseries from ADT, and
    updates the changes (last datapoint, properties, linked asset) or creates the twin if it is a new timeseries in CDF since the last sync.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets linked to the timeseries
    :param float sync_ts: timestamp of the last synchronization
    :return int: number of timeseries updated
    '''
    n = 0
    # for each asset get the linked timeseries
    for a in asset_list:
        asset_ext_id = convert_ext_id(a.external_id)
        ts_list = cdf_client.time_series.list(asset_external_ids=[a.external_id], limit=-1)
        if not(ts_list):
            continue
        # create the corresponding digital twins for each timeseries
        for t in ts_list:
            ext_id = convert_ext_id(t.external_id)
            ts = cdf_client.datapoints.retrieve_latest(external_id=t.external_id)
            if (ts):
                datapoint = ts[0]
            else:
                datapoint = None
            ##print(asset_ext_id, t, d.value, d.timestamp)
            try:
                dt = adt_client.get_digital_twin(digital_twin_id=ext_id)
                # get update patches for Timeseries resource
                update_patches = get_update_patches(t, dt)
                # check if the latest datapoint has changed and extend update patches list
                
                if 'timestamp' in dt:
                    try:
                        dt_datetime = datetime.strptime(dt['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    except:
                        dt_datetime = datetime.strptime(dt['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
                else:
                    dt_datetime = datetime.strptime('1970-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')               
                if (datapoint and (str(datapoint.value) != dt['latestValue'] or \
                    datapoint.timestamp/1000 > dt_datetime.replace(tzinfo=timezone.utc).timestamp())):
                    update_patches.append({'op': 'replace', 'path': '/latestValue', 'value': str(datapoint.value)})
                    update_patches.append({'op': 'replace', 'path': '/timestamp', 'value': datetime.fromtimestamp(datapoint.timestamp/1000, tz=timezone.utc)})
                
                if (update_patches):
                    n = n+1
                    adt_client.update_digital_twin(ext_id, update_patches)
                
                # check if asset linked to timeseries changed => relationship needs to be recreated
                if (t.last_updated_time > sync_ts*1000):
                    rel_list_adt = adt_client.list_incoming_relationships(ext_id)
                    rela = next((r for r in list(rel_list_adt) if (r.relationship_name == 'contains')), None)
                    if not(rela):   # relationship does not exist in ADT
                        logging.warning('Skipping linked asset update for timeseries asset "%s", because it is not provided in ADT yet!', ext_id)
                        continue
                    if (rela.source_id != asset_ext_id):    # linked asset has changed
                        if (rela.relationship_id != rela.source_id + '->' + ext_id):
                            logging.warning('The linked asset relationship with ID "%s", between the "%s" asset and "%s" timeseries twins was likely not created' + 
                                'by CDF->ADT sync, and the ADT->CDF sync might have failed. Still, updating linked asset to "%s" now!', 
                                rela.relationship_id, rela.source_id, ext_id, asset_ext_id)
                        if not(update_patches):  # twin was not updated, but linked asset was, so count this occurrence
                            n = n+1
                        adt_client.delete_relationship(rela.source_id, rela.relationship_id)
                        insert_adt_relationship(adt_client, asset_ext_id, ext_id, 'contains')
            except ResourceNotFoundError:
                n = n+1
                # this is a new digital twin
                temp_twin = get_twin_dict(t, ADT_MODEL_IDS.TIMESERIES)
                if (datapoint):
                    temp_twin['latestValue'] = str(datapoint.value)
                    temp_twin['timestamp'] = datetime.fromtimestamp(datapoint.timestamp/1000, tz=timezone.utc)
                adt_client.upsert_digital_twin(ext_id, temp_twin)
                # WARNING: external ID might have changed in CDF, this is not handled for now
                insert_adt_relationship(adt_client, asset_ext_id, ext_id, 'contains')
    return n


def delete_assets(adt_client: DigitalTwinsClient, asset_list: AssetList, root_ext_id: str) -> int:
    '''
    Deletes digital twins from ADT corresponding to assets not present in CDF anymore.
    Also, for each twin all relationships (both outgoing and indoming) are deleted from ADT.
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets
    :param str root_ext_id: The external ID of the root asset, for which the graph should be checked
    :return int: number of assets deleted
    '''
    n = 0
    asset_ext_ids = list(map(lambda x: convert_ext_id(x.external_id), asset_list))
    root_dt = adt_client.get_digital_twin(convert_ext_id(root_ext_id))
    twin_list = []          # list of all digital twins under this root node
    twin_prev = [root_dt]   # list of digital twins from the previous level of the tree (start at level 0, i.e. root node)
    # loop through all twins from the previous level until leaf nodes are reached
    while (twin_prev):
        twin_children = []
        for dt in twin_prev:
            rels = adt_client.list_incoming_relationships(dt['$dtId'])
            for r in rels:      # find parent relationship
                if (r.relationship_name == 'parent'):   # add children to list
                    twin_children.append(adt_client.get_digital_twin(r.source_id))
        twin_list.extend(twin_children)
        twin_prev = twin_children
    # delete assets from ADT that are not present in CDF
    # together with all relationships (both outgoing and incoming)
    for i in range(len(twin_list)-1, -1, -1):
        dt_id = twin_list[i]['$dtId']
        if (dt_id not in asset_ext_ids):
            n = n+1
            rels = adt_client.list_relationships(dt_id)
            for r in rels:
                adt_client.delete_relationship(dt_id, r['$relationshipId'])
            rels = adt_client.list_incoming_relationships(dt_id)
            for r in rels:
                adt_client.delete_relationship(r.source_id, r.relationship_id)
            adt_client.delete_digital_twin(dt_id)
    return n


def delete_asset_to_asset_relationships(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, asset_list: AssetList) -> int:
    '''
    Deletes asset-to-asset relationships from ADT that are not present in CDF anymore.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets to check relationships between
    :return int: number of relationships deleted
    '''
    n = 0
    asset_ext_ids = list(map(lambda x: x.external_id, asset_list))
    rel_list_cdf = cdf_client.relationships.list(source_external_ids=asset_ext_ids, target_external_ids=asset_ext_ids, limit=-1)
    rel_ext_ids = list(map(lambda x: x.external_id, rel_list_cdf))
    # retrieve all relationships from ADT
    sql_query = 'SELECT R FROM DIGITALTWINS T JOIN CT RELATED T.relatesTo R WHERE T.$dtId in ' + SQL_PLACEHOLDER
    rel_list_adt = query_adt_batches(adt_client, sql_query, list(map(lambda x: convert_ext_id(x), asset_ext_ids)))
    for r in rel_list_adt:  # delete the ones not present in CDF anymore
        if not(r['R']['$relationshipId'] in rel_ext_ids):
            n = n+1
            adt_client.delete_relationship(r['R']['$sourceId'], r['R']['$relationshipId'])
    return n


def delete_timeseries(cdf_client: CogniteClient, adt_client: DigitalTwinsClient, asset_list: AssetList) -> int:
    '''
    Deletes digital twins from ADT corresponding to timeseries not present in CDF anymore.
    For each twin the relationships connecting it to an asset is also deleted.
    :param CogniteClient cdf_client: The CDF client object
    :param DigitalTwinsClient adt_client: The ADT client object
    :param AssetList asset_list: The list of CDF assets linked to the timeseries
    :return int: number of timeseries deleted
    '''
    n = 0
    asset_ext_ids = list(map(lambda x: x.external_id, asset_list))
    ts_list_cdf = cdf_client.time_series.list(limit=-1)
    ts_ext_ids = list(map(lambda x: convert_ext_id(x.external_id), ts_list_cdf))
    # retrieve all timeseries from ADT under the given assets
    sql_query = 'SELECT T.$dtId as assetId, CT.$dtId as tsId, R.$relationshipId as relId ' + \
        'FROM DIGITALTWINS T JOIN CT RELATED T.contains R WHERE T.$dtId in ' + SQL_PLACEHOLDER + \
        ' and CT.$metadata.$model = \'' + ADT_MODEL_IDS.TIMESERIES.value + '\''
    ts_list_adt = query_adt_batches(adt_client, sql_query, list(map(lambda x: convert_ext_id(x), asset_ext_ids)))
    for ts in ts_list_adt:
        if (ts['tsId'] not in ts_ext_ids):
            n = n+1
            adt_client.delete_relationship(ts['assetId'], ts['relId'])
            adt_client.delete_digital_twin(ts['tsId'])
    return n


###############################################################################
############################## utility functions ##############################
###############################################################################
def create_twin(a: Asset, adt_client: DigitalTwinsClient) -> None:
    '''
    Creates the digital twin in ADT for the given asset from CDF, together with the implicit parent relationship.
    :param Asset a: The CDF asset object
    :param DigitalTwinsClient adt_client: The ADT client object
    :return: None
    '''
    temp_twin = get_twin_dict(a, ADT_MODEL_IDS.ASSET)
    ext_id = convert_ext_id(a.external_id)
    parent_ext_id = convert_ext_id(a.parent_external_id)
    
    try:
        adt_client.upsert_digital_twin(ext_id, temp_twin)

        if (a.parent_external_id):
            insert_adt_relationship(adt_client, ext_id, parent_ext_id, 'parent')
    
    except Exception as e:
        logging.error(f"Failed: {ext_id}")
        logging.error(e)
        raise e
    
    return


def get_twin_dict(resource: Union[Asset, TimeSeries], model: ADT_MODEL_IDS) -> dict:
    '''
    Creates the base digital twin structure for the given CDF resource.
    :param Asset|Timeseries resource: CDF resource
    :param ADT_MODEL_IDS model: a member of the class defining the possible models
    :return [dict]: digital twin dictionary
    '''
    twin_dict = {
        'displayName': resource.name,
        'externalId': resource.external_id,
        'id': str(resource.id),
        '$metadata': {
            '$model': model.value
        },
        'tags': {
            '$metadata': {},
            'values': convert_metadata(resource.metadata)
        },
    }
    if (resource.description):
        twin_dict['description'] = resource.description
    return twin_dict


def insert_adt_relationship(adt_client: DigitalTwinsClient, sourceId: str, targetId: str, rel_name: str, rel_id: str = None, labels: str = None) -> None:
    '''
    Inserts a new relationship in ADT between the given source and target and with the given name.
    :param DigitalTwinsClient adt_client: The ADT client object
    :param str sourceId: The ID of the source digital twin
    :param str targetId: The ID of the target digital twin
    :param str rel_name: The name of the relationship
    :param str rel_Id: Optional relationship ID
    :param str labels: Optional labels parameter (for explicit relatesTo relationships)
    :return None:
    '''
    if not(rel_name in ['parent', 'contains', 'relatesTo']):
        raise ValueError('Cannot insert new relationship! Unknown relationship name ' + rel_name + '!')
    if not(rel_id):
        rel_id = sourceId + '->' + targetId
    temp_rel = {
        '$relationshipId': rel_id,
        '$relationshipName': rel_name,
        '$targetId': targetId
    }
    if (labels):
        temp_rel['labels'] = labels
    adt_client.upsert_relationship(sourceId, temp_rel['$relationshipId'], temp_rel)
    return


def get_update_patches(resource: Union[Asset, TimeSeries], digital_twin: dict) -> list[dict]:
    '''
    Determines the JSON update pathes for ADT by comparing the given CDF resource with its digital twin counterpart.
    :param Asset|Timeseries resource: CDF resource
    :param dict digital_twin: ADT digital twin dictionary corre4sponding to a resource
    :return [dict]: list of JSON pathces to be updated in ADT
    '''
    p = []  # list of JSON patches to use for updating the digital twin

    # external ID and internal ID are special cases: should not be allowed to change so set only if empty
    if not('externalId' in digital_twin):
        p.append({'op': 'add', 'path': '/externalId', 'value': resource.external_id})
    elif (digital_twin['externalId'] == ''):
        p.append({'op': 'replace', 'path': '/externalId', 'value': resource.external_id})
    elif (resource.external_id != digital_twin['externalId']):
        logging.warning('CDF external ID "%s" should match ADT external ID property "%s". Not updating!', resource.external_id, digital_twin['externalId'])
    if not('id' in digital_twin):
        p.append({'op': 'add', 'path': '/id', 'value': str(resource.id)})
    elif (digital_twin['id'] == ''):
        p.append({'op': 'replace', 'path': '/id', 'value': str(resource.id)})
    elif (str(resource.id) != digital_twin['id']):
        logging.warning('CDF internal ID "%s" should match ADT "id" property "%s". Not updating!', str(resource.id), digital_twin['id'])

    # name and description are simple cases
    if not('displayName' in digital_twin):
        p.append({'op': 'add', 'path': '/displayName', 'value': resource.name})
    elif (resource.name != digital_twin['displayName']):
        p.append({'op': 'replace', 'path': '/displayName', 'value': resource.name})
    if not(resource.description):  # description may not exist yet/anymore
        if ('description' in digital_twin):
            p.append({'op': 'remove', 'path': '/description'})
    elif not('description' in digital_twin):
        p.append({'op': 'add', 'path': '/description', 'value': resource.description})
    elif (resource.description != digital_twin['description']):
        p.append({'op': 'replace', 'path': '/description', 'value': resource.description})
    
    # CDF metadata is a dictionary itself, and the 'tags' is a map in ADT
    if (not('values' in digital_twin['tags'])):
        digital_twin['tags']['values'] = {}   # create the tags if it does not exist
    meta = convert_metadata(resource.metadata)
    if (meta != digital_twin['tags']['values']):
        for k in meta:
            if not(k in digital_twin['tags']['values']):
                p.append({'op': 'add', 'path': '/tags/values/' + k, 'value': meta[k]})                    
            elif (meta[k] != digital_twin['tags']['values'][k]):
                p.append({'op': 'replace', 'path': '/tags/values/' + k, 'value': meta[k]})
        for k in digital_twin['tags']['values']:
            if not(k in meta):
                p.append({'op': 'remove', 'path': '/tags/values/' + k})
    return p    


def query_adt_batches(adt_client: DigitalTwinsClient, sql_template: str, predicates: list[str]) -> list[dict[str, object]]:
    '''
    Performs the SQL query in ADT in batches (bacause the list length for the IN/NIN operation cannot exceed 100 elements).
    :param DigitalTwinsClient adt_client: The ADT client object
    :param str sql_template: the SQL query with a placeholder template to be replaced by batches of the list
    :param list[str] predicates: the list to be divided into batches
    :return list: the concatenated result of the SQL queries
    '''
    res_adt = []
    n = SQL_IN_BATCH_SIZE
    for pred_batch in [predicates[i:i+n] for i in range(0, len(predicates), n)]:
        sql_query = sql_template.replace(SQL_PLACEHOLDER, str(pred_batch))
        res_adt_batch = adt_client.query_twins(sql_query)
        res_adt.extend(list(res_adt_batch))
    return res_adt


def convert_ext_id(external_id: str) -> str:
    '''
    Converts CDF external ID to valid ADT ID by replacing the following problematic characters:
        ':'     ->  '*'
        <space> ->  '_'
    WARNING: this is a temporary solution
        (not sure if this should be handled at all, could just thow error for the whole conversion)
    '''
    if (external_id):
        return external_id.replace(':', '*').replace(' ', '_').replace('&','AND')
    else:
        return external_id


def convert_metadata(metadata: dict) -> dict:
    '''
    Converts CDF metadata to valid ADT format, replacing problematic characters in map keys:
        <space> ->  '_'
        '.'     ->  '^'
        '$'     ->  '#'
    WARNING: temporary solution
    '''
    new_map = {}
    for k in metadata:
        kk = k.replace(' ', '_').replace('.', '^').replace('$', '#')
        new_map[kk] = metadata[k]
    return new_map


def get_last_exec_file() -> Tuple[BlobClient, dict]:
    '''
    Retrieves the contents of the file stored in the Azure blob storage, describing information 
    about the last time the CDF->ADT synchronization was executed: timestamp in seconds for each root asset.
    '''
    try:
        connect_str = os.getenv('AzureWebJobsStorage')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    except:
        logging.error('ERROR: Cannot connect to Azure Blob Storage!')
        raise

    container_client: ContainerClient
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
    if not(container_client.exists()):  # create the container now
        container_client = blob_service_client.create_container(BLOB_CONTAINER_NAME)
    blob_client: BlobClient = container_client.get_blob_client(BLOB_FILE_NAME)

    try:
        stream: StorageStreamDownloader = blob_client.download_blob()
        func_runs = json.loads(stream.readall())
        return (blob_client, func_runs)
    except ResourceNotFoundError:
        # start with empty result, i.e. this is the first time the function is executed
        return (blob_client, {'last_executions': []})


def get_last_exec_TS(ext_id: str) -> float:
    '''
    Get the timestamp of the last execution of the CDF->ADT sync for the given asset external ID.
    :param string ext_id: external ID of the root asset
    :return float: timestamp of last execution
    '''
    (_, func_runs) = get_last_exec_file()
    for x in func_runs['last_executions']:
        if (x['root_asset_ext_id'] == ext_id):
            return x['timestamp_UTC']
    return None


def set_last_exec_TS(ext_id: str, ts: float) -> None:
    '''
    Set the timestamp of the current execution of the CDF->ADT sync for the given asset external ID.
    :param string ext_id: external ID of the root asset
    :param float ts: timestamp of current execution
    '''
    (blob_client, func_runs) = get_last_exec_file()
    found = False
    for x in func_runs['last_executions']:
        if (x['root_asset_ext_id'] == ext_id):
            x['timestamp_UTC'] = ts
            found = True
    if not(found):
        func_runs['last_executions'].append({'root_asset_ext_id': ext_id, 'timestamp_UTC': ts})
    blob_client.upload_blob(json.dumps(func_runs, indent=4), overwrite=True)
    return


def get_cdf_client() -> CogniteClient:
    '''
    Retrieves a Cognite Data Fusion (CDF) client object, which allows to interact with CDF.
    Prerequisite: make sure the following environment variables are set
        CDF_TENANTID
        CDF_CLIENTID
        CDF_CLUSTER
        CDF_PROJECT
        CDF_CLIENT_SECRET
    :return: the CDF client object
    '''
    TENANT_ID = os.environ['CDF_TENANTID']
    CLIENT_ID = os.environ['CDF_CLIENTID']
    CDF_CLUSTER =  os.environ['CDF_CLUSTER']
    COGNITE_PROJECT =  os.environ['CDF_PROJECT']

    SCOPES = [f'https://{CDF_CLUSTER}.cognitedata.com/.default']

    CLIENT_SECRET = os.environ['CDF_CLIENT_SECRET']

    TOKEN_URL = 'https://login.microsoftonline.com/%s/oauth2/v2.0/token' % TENANT_ID

    BASE_URL = f"https://{CDF_CLUSTER}.cognitedata.com"

    creds = OAuthClientCredentials(token_url=TOKEN_URL, client_id=CLIENT_ID, scopes=SCOPES, client_secret=CLIENT_SECRET)
    cnf = ClientConfig(client_name="cdf-optimisation", project=COGNITE_PROJECT, credentials=creds, base_url=BASE_URL)
    cdf_client = CogniteClient(cnf)


    #print(cdf_client.iam.token.inspect())
    return cdf_client


def get_adt_client() -> DigitalTwinsClient:
    '''
    Retrieves an Azure Digital Twin (ADT) client object, which allows to interact with ADT.
    Prerequisite: make sure the 'ADT_URL' environment variable is set.
        When running locally, the following variables are also needed:
            AZURE_SUBSCRIPTION_ID
            AZURE_TENANT_ID
            AZURE_CLIENT_ID
    :return: the ADT client object
    '''
    url = os.environ['ADT_URL']
    credential = DefaultAzureCredential()
    adt_client = DigitalTwinsClient(url, credential)
    return adt_client
