import json
from abc import ABC
from typing import Any, Dict, List, Mapping, Optional, Union

import dagster._check as check
from enum import Enum


class AirbyteSyncMode(ABC):
    """
    Represents the sync mode for a given Airbyte stream.
    """

    def to_json(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, AirbyteSyncMode) and self.to_json() == other.to_json()

    @classmethod
    def full_refresh_append(cls) -> "AirbyteSyncMode":
        return _FullRefreshAppend()

    @classmethod
    def full_refresh_overwrite(cls) -> "AirbyteSyncMode":
        return _FullRefreshOverwrite()

    @classmethod
    def incremental_append(
        cls,
        cursor_field: Optional[str] = None,
    ) -> "AirbyteSyncMode":
        return _IncrementalAppend(cursor_field)

    @classmethod
    def incremental_append_dedup(
        cls,
        cursor_field: Optional[str] = None,
        primary_key: Optional[Union[str, List[str]]] = None,
    ) -> "AirbyteSyncMode":
        return _IncrementalAppendDedup(cursor_field=cursor_field, primary_key=primary_key)


class _LoadedSyncMode(AirbyteSyncMode):
    def __init__(self, dict: Dict[str, Any]):
        self.dict = {
            k: v
            for k, v in dict.items()
            if k in ("syncMode", "destinationSyncMode", "cursorField", "primaryKey")
        }

    def to_json(self) -> Dict[str, Any]:
        return self.dict


class _BasicSyncMode(AirbyteSyncMode):
    def __init__(self, sync_mode: str, destination_sync_mode: str):
        self.sync_mode = check.str_param(sync_mode, "sync_mode")
        self.destination_sync_mode = check.str_param(destination_sync_mode, "destination_sync_mode")

    def to_json(self) -> Dict[str, Any]:
        return {
            "syncMode": self.sync_mode,
            "destinationSyncMode": self.destination_sync_mode,
        }


class _FullRefreshAppend(_BasicSyncMode):
    def __init__(self):
        super().__init__("full_refresh", "append")


class _FullRefreshOverwrite(_BasicSyncMode):
    def __init__(self):
        super().__init__("full_refresh", "overwrite")


class _IncrementalAppend(_BasicSyncMode):
    def __init__(self, cursor_field: Optional[str] = None):
        super().__init__("incremental", "append")
        self.cursor_field = check.opt_str_param(cursor_field, "cursor_field")

    def to_json(self) -> Dict[str, Any]:
        return {
            **super().to_json(),
            **({"cursorField": self.cursor_field} if self.cursor_field else {}),
        }


class _IncrementalAppendDedup(_BasicSyncMode):
    def __init__(
        self,
        cursor_field: Optional[str] = None,
        primary_key: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__("incremental", "append_dedup")
        self.cursor_field = [check.opt_str_param(cursor_field, "cursor_field")]
        if isinstance(primary_key, str):
            primary_key = [primary_key]
        self.primary_key = [
            [x] for x in check.opt_list_param(primary_key, "primary_key", of_type=str)
        ]

    def to_json(self) -> Dict[str, Any]:
        return {
            **super().to_json(),
            **({"cursorField": self.cursor_field} if self.cursor_field else {}),
            **({"primaryKey": self.primary_key} if self.primary_key else {}),
        }


class AirbyteSource:
    """
    Represents a user-defined Airbyte source.
    """

    def __init__(self, name: str, source_type: str, source_configuration: Mapping[str, Any]):
        self.name = check.str_param(name, "name")
        self.source_type = check.str_param(source_type, "source_type")
        self.source_configuration = check.dict_param(
            source_configuration, "source_configuration", key_type=str
        )

    def must_be_recreated(self, other: "AirbyteSource") -> bool:
        return False
        # return self.name != other.name or self.source_configuration != other.source_configuration


class InitializedAirbyteSource:
    """
    User-defined Airbyte source bound to actual created Airbyte source.
    """

    def __init__(self, source: AirbyteSource, source_id: str, source_definition_id: Optional[str]):
        self.source = source
        self.source_id = source_id
        self.source_definition_id = source_definition_id

    @classmethod
    def from_api_json(cls, api_json: Mapping[str, Any]):
        return cls(
            source=AirbyteSource(
                name=api_json["name"],
                source_type=api_json["sourceName"],
                source_configuration=api_json["connectionConfiguration"],
            ),
            source_id=api_json["sourceId"],
            source_definition_id=None,
        )


class AirbyteDestination:
    """
    Represents a user-defined Airbyte destination.
    """

    def __init__(
        self, name: str, destination_type: str, destination_configuration: Mapping[str, Any]
    ):
        self.name = check.str_param(name, "name")
        self.destination_type = check.str_param(destination_type, "destination_type")
        self.destination_configuration = check.dict_param(
            destination_configuration, "destination_configuration", key_type=str
        )

    def must_be_recreated(self, other: "AirbyteDestination") -> bool:
        return False
        # return (
        #     self.name != other.name
        #     or self.destination_configuration != other.destination_configuration
        # )


class InitializedAirbyteDestination:
    """
    User-defined Airbyte destination bound to actual created Airbyte destination.
    """

    def __init__(
        self,
        destination: AirbyteDestination,
        destination_id: str,
        destination_definition_id: Optional[str],
    ):
        self.destination = destination
        self.destination_id = destination_id
        self.destination_definition_id = destination_definition_id

    @classmethod
    def from_api_json(cls, api_json: Mapping[str, Any]):
        return cls(
            destination=AirbyteDestination(
                name=api_json["name"],
                destination_type=api_json["destinationName"],
                destination_configuration=api_json["connectionConfiguration"],
            ),
            destination_id=api_json["destinationId"],
            destination_definition_id=None,
        )


class AirbyteDestinationNamespace(Enum):
    """
    Represents the sync mode for a given Airbyte stream.
    """

    SAME_AS_SOURCE = "source"
    DESTINATION_DEFAULT = "destination"


class AirbyteConnection:
    """
    User-defined Airbyte connection.
    """

    def __init__(
        self,
        name: str,
        source: AirbyteSource,
        destination: AirbyteDestination,
        stream_config: Mapping[str, AirbyteSyncMode],
        normalize_data: Optional[bool] = None,
        destination_namespace: Optional[
            Union[AirbyteDestinationNamespace, str]
        ] = AirbyteDestinationNamespace.SAME_AS_SOURCE,
    ):
        self.name = check.str_param(name, "name")
        self.source = check.inst_param(source, "source", AirbyteSource)
        self.destination = check.inst_param(destination, "destination", AirbyteDestination)
        self.stream_config = check.dict_param(
            stream_config, "stream_config", key_type=str, value_type=AirbyteSyncMode
        )
        self.normalize_data = check.opt_bool_param(normalize_data, "normalize_data")
        self.destination_namespace = check.opt_inst_param(
            destination_namespace, "destination_namespace", (str, AirbyteDestinationNamespace)
        )

    def must_be_recreated(self, other: Optional["AirbyteConnection"]) -> bool:
        return (
            not other
            or self.source.must_be_recreated(other.source)
            or self.destination.must_be_recreated(other.destination)
        )


class InitializedAirbyteConnection:
    """
    User-defined Airbyte connection bound to actual created Airbyte connection.
    """

    def __init__(
        self,
        connection: AirbyteConnection,
        connection_id: str,
    ):
        self.connection = connection
        self.connection_id = connection_id

    @classmethod
    def from_api_json(
        cls,
        api_dict: Mapping[str, Any],
        init_sources: Mapping[str, InitializedAirbyteSource],
        init_dests: Mapping[str, InitializedAirbyteDestination],
    ):

        source = next(
            (
                source.source
                for source in init_sources.values()
                if source.source_id == api_dict["sourceId"]
            ),
            None,
        )
        dest = next(
            (
                dest.destination
                for dest in init_dests.values()
                if dest.destination_id == api_dict["destinationId"]
            ),
            None,
        )

        source = check.not_none(source, f"Could not find source with id {api_dict['sourceId']}")
        dest = check.not_none(
            dest, f"Could not find destination with id {api_dict['destinationId']}"
        )

        streams = {
            stream["stream"]["name"]: _LoadedSyncMode(stream["config"])
            for stream in api_dict["syncCatalog"]["streams"]
        }
        return cls(
            AirbyteConnection(
                name=api_dict["name"],
                source=source,
                destination=dest,
                stream_config=streams,
                normalize_data=len(api_dict["operationIds"]) > 0,
                destination_namespace=api_dict["namespaceFormat"]
                if api_dict["namespaceDefinition"] == "customformat"
                else AirbyteDestinationNamespace(api_dict["namespaceDefinition"]),
            ),
            api_dict["connectionId"],
        )


def _remove_none_values(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in obj.items() if v is not None}


def _dump_class(obj: Any) -> Dict[str, Any]:
    return json.loads(json.dumps(obj, default=lambda o: _remove_none_values(o.__dict__)))


class GeneratedAirbyteSource(AirbyteSource):
    """
    Base class used by the codegen Airbyte sources. This class is not intended to be used directly.

    Converts all of its attributes into a source configuration dict which is passed down to the base
    AirbyteSource class.
    """

    def __init__(self, source_type: str, name: str):
        source_configuration = _dump_class(self)
        super().__init__(
            name=name, source_type=source_type, source_configuration=source_configuration
        )


class GeneratedAirbyteDestination(AirbyteDestination):
    """
    Base class used by the codegen Airbyte destinations. This class is not intended to be used directly.

    Converts all of its attributes into a destination configuration dict which is passed down to the
    base AirbyteDestination class.
    """

    def __init__(self, source_type: str, name: str):
        destination_configuration = _dump_class(self)
        super().__init__(
            name=name,
            destination_type=source_type,
            destination_configuration=destination_configuration,
        )
