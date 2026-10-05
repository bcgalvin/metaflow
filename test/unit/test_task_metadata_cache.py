"""Regression tests for caller-supplied task metadata in the client cache (#3397)."""

import pytest

from metaflow.client.filecache import FileCache, TaskMetadataCache
from metaflow.datastore.flow_datastore import FlowDataStore
from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.plugins.datastores.serializers.pickle_serializer import PickleSerializer

FLOW_NAME = "TestFlow"
TASK_PATH = ("1", "end", "1")


@pytest.fixture
def completed_task(tmp_path):
    flow_ds = FlowDataStore(
        FLOW_NAME, storage_impl=LocalStorage, ds_root=str(tmp_path / "datastore")
    )
    task_ds = flow_ds.get_task_datastore(*TASK_PATH, attempt=0, mode="w")
    task_ds.init_task()
    # Pin the writer serializer so globally registered test serializers don't affect this fixture.
    task_ds._serializers = [PickleSerializer]
    task_ds.save_artifacts(iter([("result", 123)]))
    task_ds.done()
    return flow_ds


@pytest.fixture
def client_cache(tmp_path):
    return FileCache(cache_dir=str(tmp_path / "client-cache"))


@pytest.mark.parametrize("warm_cache", [False, True], ids=["cold", "warm"])
@pytest.mark.parametrize("log_format", ["mflog", "legacy"])
def test_log_size_preserves_artifact_sizes(
    completed_task, client_cache, warm_cache, log_format
):
    ds_root = completed_task.datastore_root
    artifact_args = ("local", ds_root, 0, FLOW_NAME, *TASK_PATH, "result")
    if warm_cache:
        assert client_cache.get_artifact_size(*artifact_args) == 5

    if log_format == "mflog":
        client_cache.get_log_size("local", ds_root, "stdout", 0, FLOW_NAME, *TASK_PATH)
    else:
        location = LocalStorage.path_join(
            ds_root, FLOW_NAME, *TASK_PATH, "0.stdout.log"
        )
        client_cache.get_legacy_log_size(
            "local", location, "stdout", 0, FLOW_NAME, *TASK_PATH
        )

    assert client_cache.get_artifact_size(*artifact_args) == 5


def test_artifact_size_caches_metadata_once(completed_task, client_cache, mocker):
    store = mocker.spy(TaskMetadataCache, "store_metadata")
    artifact_args = (
        "local",
        completed_task.datastore_root,
        0,
        FLOW_NAME,
        *TASK_PATH,
        "result",
    )
    assert client_cache.get_artifact_size(*artifact_args) == 5
    assert client_cache.get_artifact_size(*artifact_args) == 5
    store.assert_called_once()
