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
    origin_ds_set=None,
    attempt_id=0,
):
    print(
        f"Cloning task from {flow_name}/{clone_run_id}/{step_name}/{task_id} to {flow_name}/{run_id}/{step_name}/{task_id}"
    )
    # 1. initialize output datastore
    output = flow_datastore.get_task_datastore(
        run_id, step_name, task_id, attempt=attempt_id, mode="w"
    )
    output.init_task()

    origin_run_id, origin_step_name, origin_task_id = clone_run_id, step_name, task_id
    # 2. initialize origin datastore
    origin = None
    if origin_ds_set:
        origin = origin_ds_set.get_with_pathspec(
            f"{origin_run_id}/{origin_step_name}/{origin_task_id}"
        )
    else:
        origin = flow_datastore.get_task_datastore(
            origin_run_id, origin_step_name, origin_task_id
        )
    metadata_tags = ["attempt_id:{0}".format(attempt_id)]
    output.clone(origin)
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
    output.done()
