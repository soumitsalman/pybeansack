import datetime
import json
import logging
import os
from uuid import uuid4
from icecream import ic
from pymongo import MongoClient
from pymongo.collection import Collection
from azure.data.tables import TableServiceClient, UpdateMode
# from azure.storage.blob import BlobServiceClient
from slack_sdk.oauth.installation_store import InstallationStore
from slack_sdk.oauth.installation_store.models import Installation
from slack_sdk.oauth.state_store import OAuthStateStore


_BLANK_ENTERPRISE_ID = "blank-enterprise-id"
_BLANK_TEAM_ID = "blank-team-id"
_DEFAULT_STORE = "slackapp"
_INSTALLATIONS = "installations"
_STATES = "oauthstates"

get_enterprise_id = lambda val: val if val else _BLANK_ENTERPRISE_ID
get_team_id = lambda val: val if val else _BLANK_TEAM_ID

class MongoInstallationStore(InstallationStore):
    def __init__(self, conn_str: str = os.getenv("MONGODB_CONNECTION_STRING"), app_name: str = _DEFAULT_STORE):
        client: MongoClient = MongoClient(conn_str)
        self.collection: Collection = client[app_name][_INSTALLATIONS]

    def save(self, installation: Installation):
        filter = { "team_id": installation.team_id }
        if installation.enterprise_id:
            filter["enterprise_id"] = installation.enterprise_id
        self.collection.replace_one(
            filter=ic(filter),
            replacement=installation.__dict__,
            upsert=True
        )

    def find_installation(self, *, enterprise_id: str | None, team_id: str | None, user_id: str | None = None, is_enterprise_install: bool | None = False) -> Installation | None:
        filter = { "team_id": team_id }
        if is_enterprise_install:
            filter["enterprise_id"] = enterprise_id
        res = self.collection.find_one(filter, {"_id": 0})
        return Installation(**res) if res else None

    def delete_installation(self, *, enterprise_id: str | None, team_id: str | None, user_id: str | None = None) -> None:
        filter = { "team_id": team_id }
        if enterprise_id:
            filter["enterprise_id"] = enterprise_id
        self.collection.delete_one(filter=ic(filter))

class MongoOauthStateStore(OAuthStateStore):
    def __init__(self, conn_str: str = os.getenv("MONGODB_CONNECTION_STRING"), app_name: str = _DEFAULT_STORE, expiration_seconds=3600) -> None:
        client: MongoClient = MongoClient(conn_str)
        self.collection: Collection = client[app_name][_STATES]
        self.expire_duration = expiration_seconds
    
    def issue(self, *args, **kwargs) -> str:
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds = self.expire_duration)
        state = str(uuid4())
        self.collection.insert_one({"state": state, "expire_at": expire_at})
        return state
    
    def consume(self, state: str) -> bool:
        result = self.collection.find_one_and_delete(
            { 
                "state": state, 
                "expire_at": { "$gte": datetime.datetime.now() }
            },
            { "_id": 0 }
        )
        return ic(result) != None
    

class AzureTableInstallationStore(InstallationStore):
    def __init__(self, conn_str: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING"), app_name: str = _DEFAULT_STORE) -> None:
        self.table = TableServiceClient.from_connection_string(conn_str=conn_str).create_table_if_not_exists(table_name=f"{app_name}{_INSTALLATIONS}")
        
    def save(self, installation: Installation):
        entity = {
            "PartitionKey": get_enterprise_id(installation.enterprise_id),
            "RowKey": get_team_id(installation.team_id),
            "installation" : json.dumps(installation.__dict__, indent=2)
        }
        self.table.upsert_entity(entity=entity, mode=UpdateMode.REPLACE)
        
    def find_installation(self, *, enterprise_id: str | None, team_id: str | None, user_id: str | None = None, is_enterprise_install: bool | None = False) -> Installation | None:
        try:
            install_data = self.table.get_entity(
                partition_key=get_enterprise_id(enterprise_id),
                row_key=get_team_id(team_id),
                select=["installation"])['installation']
            return Installation(**json.loads(install_data)) if install_data else None
        except Exception as err:
            logging.info("Installation Not Found", err)
            return None
    
    def delete_installation(self, *, enterprise_id: str | None, team_id: str | None, user_id: str | None = None) -> None:
        try:
            self.table.delete_entity(
                partition_key=get_enterprise_id(enterprise_id),
                row_key=get_team_id(team_id)
            )
        except Exception as err:
            logging.info("Installation Not Found", err)
    

class AzureTableOauthStateStore(OAuthStateStore):
    def __init__(self, conn_str: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING"), app_name: str = _DEFAULT_STORE, expiration_seconds=3600) -> None:
        self.table = TableServiceClient.from_connection_string(conn_str=conn_str).create_table_if_not_exists(table_name=f"{app_name}{_STATES}")
        self.expire_duration = expiration_seconds

    def issue(self, *args, **kwargs) -> str:
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds = self.expire_duration)
        state = str(uuid4())
        self.table.upsert_entity(
            {
                "PartitionKey": _STATES,
                "RowKey": state,
                "expire_at": expire_at.timestamp()
            }
        )
        return state
    
    def consume(self, state: str) -> bool:
        try:
            result = self.table.get_entity(partition_key=_STATES, row_key=state)['expire_at']
            self.table.delete_entity(partition_key=_STATES, row_key=state)    
            return ic(result) >= datetime.datetime.now().timestamp()
        except Exception as err:
            ic("State not found or already consumed", err)
            return False
    
# _blob_name = lambda ent_id, team_id: f"{ent_id}-{team_id}.json"
# class _UNTESTED_AzureBlobInstallationStore(InstallationStore):
#     def __init__(self, conn_str: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING"), app_name: str = _DEFAULT_STORE) -> None:
#         client = BlobServiceClient.from_connection_string(conn_str=conn_str)
#         self.container = client.get_container_client(f"{app_name}-{_INSTALLATIONS}")

#     def save(self, installation: Installation):
#         blob_name = _blob_name(installation.enterprise_id, installation.team_id)
#         data = json.dumps(installation.to_dict(), indent=2)
#         self.container.get_blob_client(blob=blob_name).upload_blob(data = data, overwrite=True)
    
#     def find_installation(self, *, enterprise_id: str | None, team_id: str | None, user_id: str | None = None, is_enterprise_install: bool | None = False) -> Installation | None:
#         blob_name = _blob_name(enterprise_id, team_id)
#         try:
#             blob_data = self.container.get_blob_client(blob=blob_name).download_blob().content_as_text()
#             install_data = json.loads(blob_data)
#             return Installation(**install_data) if install_data else None
#         except Exception as err:
#             logging.info("Installation Not Found", err)
#             return None
    
#     def delete_installation(self, *, enterprise_id: str | None, team_id: str | None, user_id: str | None = None) -> None:
#         blob_name = _blob_name(enterprise_id, team_id)
#         try:
#             blob_client = self.container.get_blob_client(blob=blob_name)
#             blob_client.delete_blob()
#         except Exception as err:
#             logging.info("Installation Not Found", err)