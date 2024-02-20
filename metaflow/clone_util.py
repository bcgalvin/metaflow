import time
from .metadata import MetaDatum


def clone_task_helper(
    flow_name,
    clone_run_id,
    run_id,
    step_name,
    task_id,
    flow_datastore,
    metadata_service,
    attempt_id=0,
):
    print(
        f"Cloning task from {flow_name}/{clone_run_id}/{step_name}/{task_id} to {flow_name}/{run_id}/{step_name}/{task_id}"
    )
    start_time = time.time()
    # 1. initialize output datastore
    output = flow_datastore.get_task_datastore(
        run_id, step_name, task_id, attempt=attempt_id, mode="w"
    )
    output.init_task()
    end_time = time.time()
    print(
        f"Cloning task {flow_name}/{run_id}/{step_name}/{task_id}, step 1, finished in {end_time - start_time:.2f} secs"
    )

    origin_run_id, origin_step_name, origin_task_id = clone_run_id, step_name, task_id
    # 2. initialize origin datastore
    origin = flow_datastore.get_task_datastore(
        origin_run_id, origin_step_name, origin_task_id
    )
    metadata_tags = ["attempt_id:{0}".format(attempt_id)]
    output.clone(origin)
    end_time = time.time()
    print(
        f"Cloning task {flow_name}/{run_id}/{step_name}/{task_id}, step 2.1, finished in {end_time - start_time:.2f} secs"
    )
    _ = metadata_service.register_task_id(
        run_id,
        step_name,
        task_id,
        attempt_id,
    )
    metadata_service.register_metadata(
        run_id,
        step_name,
        task_id,
        [
            MetaDatum(
                field="origin-task-id",
                value=str(origin_task_id),
                type="origin-task-id",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="origin-run-id",
                value=str(origin_run_id),
                type="origin-run-id",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="attempt",
                value=str(attempt_id),
                type="attempt",
                tags=metadata_tags,
            ),
        ],
    )
    end_time = time.time()
    print(
        f"Cloning task {flow_name}/{run_id}/{step_name}/{task_id}, step 2.2, finished in {end_time - start_time:.2f} secs"
    )
    output.done()
    end_time = time.time()
    print(
        f"Cloning task {flow_name}/{run_id}/{step_name}/{task_id} finished in {end_time - start_time:.2f} secs"
    )


def print_hello(flow_name, clone_run_id, run_id, step_name, task_id, datastore_root):

    import time

    print("Hello from clone_util")
    start_time = time.time()
    from .metaflow_config import (
        DEFAULT_DATASTORE,
        DEFAULT_ENVIRONMENT,
        DEFAULT_METADATA,
    )
    from .plugins import (
        DATASTORES,
        ENVIRONMENTS,
        METADATA_PROVIDERS,
    )
    from .datastore import FlowDataStore
    from .metaflow_environment import MetaflowEnvironment

    environment = [
        e for e in ENVIRONMENTS + [MetaflowEnvironment] if e.TYPE == DEFAULT_ENVIRONMENT
    ][0](None)
    print(f"Create environment finished at {time.time() - start_time:.2f} secs")

    metadata_service = [m for m in METADATA_PROVIDERS if m.TYPE == DEFAULT_METADATA][0](
        environment, flow_name, None, None
    )
    print(f"Create metadata finished at {time.time() - start_time:.2f} secs")

    datastore_impl = [d for d in DATASTORES if d.TYPE == DEFAULT_DATASTORE][0]
    datastore_impl.datastore_root = datastore_root

    FlowDataStore.default_storage_impl = datastore_impl
    flow_datastore = FlowDataStore(
        flow_name,
        environment,
        metadata_service,
        None,
        None,
    )
    print(f"Create FlowDataStore finished at {time.time() - start_time:.2f} secs")

    clone_task_helper(
        flow_name,
        clone_run_id,
        run_id,
        step_name,
        task_id,
        flow_datastore,
        metadata_service,
    )
    print(f"print helllo finished at {time.time() - start_time:.2f} secs")
