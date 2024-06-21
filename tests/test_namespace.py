import gymnasium as gym
import pytest

from minari import (
    create_namespace,
    delete_namespace,
    load_namespace_data,
    update_namespace,
)
from minari.data_collector.data_collector import DataCollector
from tests.common import (
    check_load_and_delete_dataset,
    create_dummy_dataset_with_collecter_env_helper,
)


# TODO: Namespace json should not be created if it's just empty
@pytest.mark.parametrize(
    "namespace",
    [
        "example_-123",
        "example_-123/",
        "nested/nested/nested/namespace",
        "nested/nested/nested/namespace/",
    ],
)
@pytest.mark.parametrize("description", [None, "my_description"])
@pytest.mark.parametrize("metadata", [None, {"metadata_key": [1, 2, 3]}])
def test_create_namespace(namespace, description, metadata):
    create_namespace(namespace, description, metadata)
    loaded_metadata = load_namespace_data(namespace)

    # TODO: This is a bit awkward
    if metadata is None:
        metadata = {}

    assert loaded_metadata is not None
    assert loaded_metadata == {**metadata, **{"description": description}}
    delete_namespace(namespace)


@pytest.mark.parametrize("namespace", ["test_namespace"])
def test_namespace_description_conflict(namespace):
    create_namespace(namespace, description="my description")

    # Re-creating the namespace does not raise a conflict unless the metadata conflicts
    create_namespace(namespace, description="my description")

    with pytest.raises(
        ValueError, match="Metadata for namespace 'test_namespace' already exists"
    ):
        create_namespace(namespace, description="a conflicting definition")

    create_namespace(namespace, description="a new definition", overwrite=True)
    loaded_metadata = load_namespace_data(namespace)
    assert loaded_metadata is not None
    assert loaded_metadata.get("description") == "a new definition"

    update_namespace(namespace, description="a third definition")
    loaded_metadata = load_namespace_data(namespace)
    assert loaded_metadata is not None
    assert loaded_metadata.get("description") == "a third definition"


def test_nonexistent_namespaces():
    assert load_namespace_data("non/existent/namespace") is None

    with pytest.raises(FileNotFoundError, match="does not exist"):
        delete_namespace("doesnt/exist")


@pytest.mark.parametrize("namespace", ["/", "./", "../", "/namespace"])
def test_create_invalid_namespace(namespace):
    with pytest.raises(ValueError, match="Malformed namespace"):
        create_namespace(namespace)


# TODO: Add a namespace property?
# TODO: Check if they use paramaterization everywhere as well
@pytest.mark.parametrize("namespace", ["nested/namespace"])
def test_delete_nonempty_namespace(namespace):
    dataset_id = f"{namespace}/cartpole-test-v0"
    create_namespace(namespace)

    env = gym.make("CartPole-v1")
    env = DataCollector(env)

    # TODO: Can we reduce duplication by borrowing this from somewhere?
    create_dummy_dataset_with_collecter_env_helper(dataset_id, env)

    with pytest.raises(ValueError, match="is not empty"):
        delete_namespace(namespace)

    check_load_and_delete_dataset(dataset_id)

    delete_namespace(namespace)


def test_create_dataset_in_namespace():
    pass
