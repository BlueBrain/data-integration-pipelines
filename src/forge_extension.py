from typing import Optional, List, Tuple, Dict, Union, Any

import os
import asyncio
import requests
from urllib.parse import quote_plus

from aiohttp import ClientSession

from kgforge.core.commons.actions import Action
from kgforge.core.commons.exceptions import RetrievalError
from kgforge.specializations.stores.bluebrain_nexus import catch_http_error_nexus, BlueBrainNexus
from kgforge.specializations.stores.nexus import Service
from kgforge.specializations.stores.nexus.batch_request_handler import BatchRequestHandler
from kgforge.core import Resource, KnowledgeGraphForge


def download_file(content_url: str, forge: KnowledgeGraphForge, path: Optional[str] = None) -> Union[str, bytes]:

    response_1 = requests.get(
        url=content_url,
        headers={**forge._store.service.headers, "Accept": "application/json"}
    )

    metadata = response_1.json()

    full_path = os.path.join(path, metadata["_filename"]) if path is not None else None

    if full_path and os.path.isfile(full_path):  # already exists don't download again - could be error-prone, pay attention to this
        return full_path

    headers = {**forge._store.service.headers, "Accept": "*/*"}

    response = requests.get(url=content_url, headers=headers)

    if path is None:
        return response.content

    with open(full_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=4096):
            f.write(chunk)

    return full_path


def _exists(provided_id: Union[str, List[str]], forge: KnowledgeGraphForge, is_file: bool) -> Union[Action, List[Action]]:

    store: BlueBrainNexus = forge._store

    def _exists_one(id_: str) -> Action:

        action_name = _exists_one.__name__

        if is_file:
            url = "/".join((store.service.url_files, quote_plus(id_)))
        else:
            url = Service.add_schema_and_id_to_endpoint(
                store.service.url_resources, schema_id=None, resource_id=id_
            )

        response = requests.get(url=url, headers=store.service.headers)

        try:
            catch_http_error_nexus(
                response, RetrievalError, aiohttp_error=False
            )

            return Action(action_name, True, None)
        except RetrievalError as e:
            return Action(action_name, False, e)

    def _exists_many(ids: List[str]) -> List[Optional[Resource]]:

        action_name = _exists_many.__name__

        async def create_tasks(
                semaphore: asyncio.Semaphore,
                loop: asyncio.AbstractEventLoop,
                ids_: List[str],
                service,
        ) -> Tuple[List[asyncio.Task], List[ClientSession]]:

            async def do_catch(id_: str, client_session: ClientSession) -> Action:

                if is_file:
                    url = "/".join((store.service.url_files, quote_plus(id_)))
                else:
                    url = Service.add_schema_and_id_to_endpoint(
                        store.service.url_resources, schema_id=None, resource_id=id_
                    )

                async with semaphore:

                    async with client_session.get(url=url, headers=store.service.headers) as response:
                        try:
                            catch_http_error_nexus(
                                response, RetrievalError, aiohttp_error=True
                            )
                            return Action(action_name, True, None)
                        except RetrievalError as e:
                            return Action(action_name, False, e)

            return BatchRequestHandler.create_tasks_and_sessions(loop, ids_, do_catch, callback=None)

        return BatchRequestHandler.batch_request(task_creator=create_tasks, data=ids, service=store.service)

    return _exists_one(provided_id) if isinstance(provided_id, str) else _exists_many(provided_id)


def _retrieve_file_metadata(file_id: Union[str, List[str]], forge: KnowledgeGraphForge, is_content_url=False) -> Union[List[Union[Resource, Action]], Union[Resource, Action]]:

    store: BlueBrainNexus = forge._store

    def _retrieve_file_metadata_one(id_: str) -> Union[Resource, Action]:

        action_name = _retrieve_file_metadata_one.__name__

        url = "/".join((store.service.url_files, quote_plus(id_))) if not is_content_url else id_

        try:
            return store._get_resource_sync(url, {})
        except RetrievalError as e:
            return Action(action_name, False, e)

    def _retrieve_file_metadata_many(ids: List[str]) -> List[Union[Resource, Action]]:

        action_name = _retrieve_file_metadata_many.__name__

        async def create_tasks(
                semaphore: asyncio.Semaphore,
                loop: asyncio.AbstractEventLoop,
                ids_: List[str],
                service,
        ) -> Tuple[List[asyncio.Task], List[ClientSession]]:

            def retrieve_done_callback(task: asyncio.Task):
                result = task.result()

                succeeded = not isinstance(result, Action)

                if isinstance(result, Resource):
                    store.service.synchronize_resource(
                        resource=result,
                        response=None,
                        action_name=action_name,
                        succeeded=succeeded,
                        synchronized=succeeded,
                    )

            async def do_catch(id_: str, client_session: ClientSession) -> Union[Resource, Action]:

                url = "/".join((store.service.url_files, quote_plus(id_))) if not is_content_url else id_

                async with semaphore:
                    try:
                        return await store._get_resource_async(
                            session=client_session, url=url, query_params={}
                        )
                    except RetrievalError as e:
                        return Action(action_name, False, e)

            return BatchRequestHandler.create_tasks_and_sessions(
                loop, ids_, do_catch, retrieve_done_callback
            )

        return BatchRequestHandler.batch_request(task_creator=create_tasks, data=ids, service=store.service)

    return _retrieve_file_metadata_one(file_id) if isinstance(file_id, str) else\
        (_retrieve_file_metadata_many(file_id) if isinstance(file_id, list) else None)
