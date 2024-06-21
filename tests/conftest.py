import copy
import tempfile

import pytest
from gymnasium import register, registry
from pytest import MonkeyPatch

# Note: Use minari.list_remote_datasets instead of named importing list_remote_datasets
# directly in tests, as it will evade monkeypatch
from minari.storage.hosting import list_remote_datasets as unmocked_list_remote_datasets


@pytest.fixture(autouse=True)
def tmp_dataset_dir():
    """Generate a temporary directory for Minari datasets."""
    tmp_dir = tempfile.TemporaryDirectory()
    with MonkeyPatch.context() as mp:
        mp.setenv("MINARI_DATASETS_PATH", tmp_dir.name)
        yield tmp_dir.name
    tmp_dir.cleanup()


@pytest.fixture
def register_dummy_envs():
    env_names = [
        "DummyBoxEnv",
        "DummyInfoEnv",
        "DummyInconsistentInfoEnv",
        "DummyMultiDimensionalBoxEnv",
        "DummyTupleDiscreteBoxEnv",
        "DummyDictEnv",
        "DummyTupleEnv",
        "DummyTextEnv",
        "DummyComboEnv",
    ]
    for env_name in env_names:
        register(
            id=f"{env_name}-v0",
            entry_point=f"tests.common:{env_name}",
            max_episode_steps=5,
        )

    yield

    for env_name in env_names:
        registry.pop(f"{env_name}-v0")


def mocked_list_remote(*args, **kwargs):
    results = unmocked_list_remote_datasets(*args, **kwargs)

    num_mocked_datasets = 2
    mocked_namespace = "example_namespace"

    for i in range(num_mocked_datasets):
        dataset_id, dataset = list(results.items())[i]
        new_id = f"{mocked_namespace}/{dataset_id}"
        new_dataset = copy.deepcopy(dataset)
        new_dataset["dataset_id"] = new_id
        results[new_id] = new_dataset

    return results


@pytest.fixture
def mock_namespaces(monkeypatch):
    # Need to patch all possible paths to the function
    monkeypatch.setattr("minari.list_remote_datasets", mocked_list_remote)
    monkeypatch.setattr(
        "minari.storage.hosting.list_remote_datasets", mocked_list_remote
    )
