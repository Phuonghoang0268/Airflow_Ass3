"""This test ensures that all Dags have tags, retries set to two, and no import errors."""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value

# helper function for test_file_imports
def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]

# helper function for test_dag_tags and test_dag_retries
def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]

# test the sucessfulness of the imports from the dag
@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")

# test the presence and correctness of the tags from the dag 
APPROVED_TAGS = {"data_workflow"}

@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS

# test the number of retries from the dag
@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    assert (
        dag.default_args.get("retries", None) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."

# test the presence of tasks in all dags and the need for all tasks to have the trigger_rule = all_success
@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tasks(dag_id, dag, fileloc):
    """
    test if all DAGs contain a task and all tasks use the trigger_rule all_success
    """
    assert dag.tasks, f"{dag_id} in {fileloc} has no tasks"
    for task in dag.tasks:
        t_rule = task.trigger_rule
        assert (
            t_rule == "all_success"
        ), f"{task} in {dag_id} has the trigger rule {t_rule} instead of all_success."

# test the presence of dag_id in the dags and if they are in the approved list
APPROVED_DAGS = {"process_web_log_v2"}

@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_id(dag_id, dag, fileloc):
    """
    test if all DAGs have a dag_id and if the dag_id is approved
    """
    assert dag_id, f"{dag_id} in {fileloc} has no tag_id"
    if APPROVED_DAGS:
        assert(
            dag_id in APPROVED_DAGS
        ), f"{dag_id} is not approved."

# test the tasks for the pwl dag
@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_pwl_dag_task_order(dag_id, dag, fileloc):
    """
    test the existences of the tasks and their order for the pwl dag
    """

    if(dag_id == "process_web_log_v2"):
        # test the existence of the tasks
        scan_task = dag.get_task('scan_for_log')
        assert scan_task, f"{dag_id} in {fileloc} doesn't have the task scan_for_log"
        extract_task = dag.get_task('extract_data')
        assert extract_task, f"{dag_id} in {fileloc} doesn't have the task extract_data"
        transform_task = dag.get_task('transform_data')
        assert transform_task, f"{dag_id} in {fileloc} doesn't have the task transform_data"
        load_task = dag.get_task('load_data')
        assert transform_task, f"{dag_id} in {fileloc} doesn't have the task load_data"

        # test the order of the tasks
        assert scan_task.downstream_task_ids == {"extract_data"}
        assert extract_task.downstream_task_ids == {"transform_data"}
        assert transform_task.downstream_task_ids == {"load_data"}
        assert load_task.upstream_task_ids == {"transform_data"}