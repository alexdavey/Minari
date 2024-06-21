import copy
import json
import os
import re
from typing import Any, Dict, Optional

from minari.storage import get_dataset_path


NAMESPACE_METADATA_FILE_NAME = "namespace_metadata.json"

# TODO: Need to add docstrings
# TODO: Need to add docs
# TODO: Check types, especially return types
# TODO: Add CLI


# TODO: Need to check that {"/", "", None} are all equivalent top-level namespaces
# TODO: Need to prevent making descriptions for the top-level namespace
def create_namespace(
    namespace: str,
    description: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    overwrite: bool = False,
):
    _validate_namespace(namespace)

    metadata = {} if metadata is None else copy.deepcopy(metadata)

    # TODO: Don't mutate inputs
    print(description, metadata)
    if (
        description is not None
        and "description" in metadata
        and description != metadata["description"]
    ):
        # TODO: Improve this message
        raise ValueError(
            "Namespace description conflicts with metadata['description']."
        )

    metadata["description"] = description

    directory = os.path.join(get_dataset_path(""), namespace)
    os.makedirs(directory, exist_ok=True)

    existing_metadata = load_namespace_data(namespace)

    if metadata == existing_metadata:
        return

    if existing_metadata is not None and not overwrite:
        # TODO: Is this the right kind of error?
        raise ValueError(
            f"Metadata for namespace '{namespace}' already exists. Set overwrite=True to overwrite existing metadata."
        )

    with open(os.path.join(directory, NAMESPACE_METADATA_FILE_NAME), "w") as file:
        json.dump(metadata, file)


def update_namespace(
    namespace: str,
    description: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
):
    return create_namespace(
        namespace=namespace, description=description, metadata=metadata, overwrite=True
    )


def load_namespace_data(namespace: str) -> Optional[Dict[str, Any]]:
    _validate_namespace(namespace)

    filepath = os.path.join(
        get_dataset_path(""), namespace, NAMESPACE_METADATA_FILE_NAME
    )

    if not os.path.exists(filepath):
        return None

    with open(filepath) as file:
        return json.load(file)


def delete_namespace(namespace: str):
    _validate_namespace(namespace)
    directory = os.path.join(get_dataset_path(""), namespace)

    if not os.path.isdir(directory):
        raise FileNotFoundError(f"Namespace {directory} does not exist.")

    dir_contents = os.listdir(directory)
    has_metadata = NAMESPACE_METADATA_FILE_NAME in dir_contents

    if len(dir_contents) != int(has_metadata):
        # TODO: Find the correct error type
        raise ValueError(
            f"Namespace {directory} is not empty. All datasets must be deleted first."
        )

    if has_metadata:
        os.remove(os.path.join(directory, NAMESPACE_METADATA_FILE_NAME))

    os.rmdir(directory)


def push_namespace_to_remote(namespace: str):
    _validate_namespace(namespace)
    pass


def pull_namespace_from_remote(namespace: str):
    _validate_namespace(namespace)
    pass


def _validate_namespace(namespace: str):
    regex = r"\w[-_\w/]*"
    if not re.fullmatch(regex, namespace):
        # TODO: Different error? What about gymnasium.error.Error?
        raise ValueError(f"Malformed namespace: {namespace}")
