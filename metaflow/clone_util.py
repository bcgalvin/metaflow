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
    # output.init_task()
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
    # output.done()
    end_time = time.time()
    print(
        f"Cloning task {flow_name}/{run_id}/{step_name}/{task_id} finished in {end_time - start_time:.2f} secs"
    )
