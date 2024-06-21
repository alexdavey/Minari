import importlib.metadata
import os
import shutil
import warnings
from pathlib import Path
from typing import Dict, List, Tuple, Union

from packaging.specifiers import SpecifierSet

from minari.dataset.minari_dataset import (
    MinariDataset,
    gen_dataset_id,
    parse_dataset_id,
)
from minari.dataset.minari_storage import MinariStorage
from minari.storage import hosting
from minari.storage.datasets_root_dir import get_dataset_path


# Use importlib due to circular import when: "from minari import __version__"
__version__ = importlib.metadata.version("minari")


def _list_non_hidden_dirs(path: Union[Path, str]) -> List[str]:
    """List all non-hidden subdirectories."""
    return [
        d.name for d in os.scandir(path) if d.is_dir() and (not d.name.startswith("."))
    ]


def dataset_id_sort_key(dataset_id: str) -> Tuple[str, Union[str, None], str, int]:
    """Key for sorting dataset ids first by namespace, and then alphabetically."""
    attrs = parse_dataset_id(dataset_id)
    return ("" if attrs[0] is None else attrs[0],) + attrs[1:]


def load_dataset(dataset_id: str, download: bool = False):
    """Retrieve Minari dataset from local database.

    Args:
        dataset_id (str): name id of Minari dataset
        download (bool): if `True` download the dataset if it is not found locally. Default to `False`.

    Returns:
        MinariDataset
    """
    file_path = get_dataset_path(dataset_id)
    data_path = os.path.join(file_path, "data")

    if not os.path.exists(data_path):
        if not download:
            raise FileNotFoundError(
                f"Dataset {dataset_id} not found locally at {file_path}. Use download=True to download the dataset."
            )

        hosting.download_dataset(dataset_id)

    return MinariDataset(data_path)


# TODO: Test this with dummy data
def list_local_datasets(
    latest_version: bool = False,
    compatible_minari_version: bool = False,
) -> Dict[str, Dict[str, Union[str, int, bool]]]:
    """Get the ids and metadata of all the Minari datasets in the local database.

    Args:
        latest_version (bool): if `True` only the latest version of the datasets are returned i.e. from ['door-human-v0', 'door-human-v1`], only the metadata for v1 is returned. Default to `False`.
        compatible_minari_version (bool): if `True` only the datasets compatible with the current Minari version are returned. Default to `False`.

    Returns:
       Dict[str, Dict[str, str]]: keys the names of the Minari datasets and values the metadata
    """
    datasets_path = get_dataset_path("")

    dataset_ids = []

    for dir_name in _list_non_hidden_dirs(datasets_path):
        dir_path = os.path.join(datasets_path, dir_name)

        if "data" in os.listdir(dir_path):
            # Minari datasets must contain the data directory.
            dataset_ids.append(dir_name)
        else:
            # Recurse one level down to check for grouped datasets
            for subdir_name in _list_non_hidden_dirs(dir_path):
                sub_dir_path = os.path.join(dir_path, subdir_name)

                if "data" in os.listdir(sub_dir_path):
                    dataset_ids.append(f"{dir_name}/{subdir_name}")

    dataset_ids = sorted(dataset_ids, key=dataset_id_sort_key)

    local_datasets = {}
    for dst_id in dataset_ids:
        data_path = os.path.join(datasets_path, dst_id, "data")
        try:
            metadata = MinariStorage.read(data_path).metadata
        except Exception as e:
            warnings.warn(f"Misconfigured dataset named {dst_id}: {e}")
            continue

        if ("minari_version" not in metadata) or (
            compatible_minari_version
            and __version__ not in SpecifierSet(metadata["minari_version"])
        ):
            continue
        namespace, env_name, dataset_name, version = parse_dataset_id(dst_id)
        dataset = gen_dataset_id(namespace, env_name, dataset_name)
        if latest_version:
            if dataset not in local_datasets or version > local_datasets[dataset][0]:
                local_datasets[dataset] = (version, metadata)
        else:
            local_datasets[dst_id] = metadata

    if latest_version:
        # Return dict = {'dataset_id': metadata}
        return dict(
            map(lambda x: (f"{x[0]}-v{x[1][0]}", x[1][1]), local_datasets.items())
        )
    else:
        return local_datasets


def delete_dataset(dataset_id: str):
    """Delete a Minari dataset from the local Minari database.

    Args:
        dataset_id (str): name id of the Minari dataset
    """
    dataset_path = get_dataset_path(dataset_id)
    shutil.rmtree(dataset_path)
    print(f"Dataset {dataset_id} deleted!")
