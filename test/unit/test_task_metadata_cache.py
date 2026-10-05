"""Regression tests for caller-supplied task metadata in the client cache (#3397)."""

import pytest

from metaflow.client.filecache import FileCache
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
    # Other test modules register serializers globally; use the normal pickle one.
    task_ds._serializers = [PickleSerializer]
    task_ds.save_artifacts(iter([("result", 123)]))
    task_ds.done()
    return flow_ds


@pytest.fixture
def client_cache(tmp_path):
    return FileCache(cache_dir=str(tmp_path / "client-cache"))


@pytest.mark.parametrize("warm_cache", [False, True], ids=["cold", "warm"])
@pytest.mark.parametrize("stream", ["stdout", "stderr"])
@pytest.mark.parametrize("log_format", ["mflog", "legacy"])
def test_log_size_preserves_artifact_sizes(
    completed_task, client_cache, warm_cache, stream, log_format
):
    ds_root = completed_task.datastore_root
    artifact_args = ("local", ds_root, 0, FLOW_NAME, *TASK_PATH, "result")
    if warm_cache:
        assert client_cache.get_artifact_size(*artifact_args) == 5

    if log_format == "mflog":
        assert (
            client_cache.get_log_size(
                "local", ds_root, stream, 0, FLOW_NAME, *TASK_PATH
            )
            == 0
        )
    else:
        location = LocalStorage.path_join(
            ds_root, FLOW_NAME, *TASK_PATH, "0.%s.log" % stream
        )
        assert (
            client_cache.get_legacy_log_size(
                "local", location, stream, 0, FLOW_NAME, *TASK_PATH
            )
            is None
        )

    assert client_cache.get_artifact_size(*artifact_args) == 5
    # A second client sharing the cache directory must still read the size.
    another_client = FileCache(cache_dir=client_cache.cache_dir)
    assert another_client.get_artifact_size(*artifact_args) == 5


@pytest.mark.parametrize("supplied_artifacts", [False, True], ids=["empty", "real"])
@pytest.mark.parametrize("spin", [False, True], ids=["regular", "spin"])
def test_supplied_metadata_is_used_without_being_cached(
    completed_task, client_cache, mocker, supplied_artifacts, spin
):
    flow_ds = client_cache._get_flow_datastore(
        "local", completed_task.datastore_root, FLOW_NAME
    )
    metadata = (
        completed_task.get_task_datastore(*TASK_PATH, attempt=0).ds_metadata
        if supplied_artifacts
        else {"objects": {}, "info": {}}
    )
    store = mocker.spy(flow_ds._metadata_cache, "store_metadata")
    kwargs = {}
    if spin:
        kwargs["orig_flow_datastore"] = flow_ds
        kwargs["spin_artifacts"] = {}
        flow_ds = completed_task
    task_ds = flow_ds.get_task_datastore(
        *TASK_PATH, attempt=0, data_metadata=metadata, **kwargs
    )

    if supplied_artifacts:
        assert task_ds["result"] == 123
    else:
        assert list(task_ds.items()) == []
    store.assert_not_called()


@pytest.mark.parametrize(
    ("attempt", "allow_not_done", "expected_writes"),
    [(0, False, 1), (None, False, 0), (0, True, 0)],
    ids=["specific-attempt", "latest-attempt", "allow-not-done"],
)
def test_metadata_loaded_from_task_retains_cache_gates(
    completed_task, client_cache, mocker, attempt, allow_not_done, expected_writes
):
    flow_ds = client_cache._get_flow_datastore(
        "local", completed_task.datastore_root, FLOW_NAME
    )
    store = mocker.spy(flow_ds._metadata_cache, "store_metadata")
    for _ in range(2):
        task_ds = flow_ds.get_task_datastore(
            *TASK_PATH, attempt=attempt, allow_not_done=allow_not_done
        )
        assert list(task_ds.get_artifact_sizes(["result"])) == [("result", 5)]
    assert store.call_count == expected_writes
