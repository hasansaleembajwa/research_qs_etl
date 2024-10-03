

from os import PathLike, fspath
from typing import TYPE_CHECKING, Iterator, Optional, Union

from pydantic import StrictStr

from snowflake.core._common import (
    CreateMode,
    SchemaObjectCollectionParent,
    SchemaObjectReferenceMixin,
)
from snowflake.core._internal.telemetry import api_telemetry
from snowflake.core._internal.utils import deprecated
from snowflake.core.stage._generated.api import StageApi
from snowflake.core.stage._generated.api_client import StoredProcApiClient
from snowflake.core.stage._generated.models.stage import Stage
from snowflake.core.stage._generated.models.stage_file import StageFile


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class StageCollection(SchemaObjectCollectionParent["StageResource"]):
    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, StageResource)
        self._api = StageApi(
            root=self.root,
            resource_class=self._ref_class,
            sproc_client=StoredProcApiClient(root=self.root),
        )

    @api_telemetry
    def create(
        self,
        stage: Stage,
        *,
        mode: CreateMode = CreateMode.error_if_exists,
    ) -> "StageResource":
        """Create a stage.

        Args:
            stage: The stage object, together with the stage's properties, object parameters.
            mode: One of the following strings.
                CreateMode.error_if_exists: Throw an :class:`snowflake.core.exceptions.ConflictError`
                if the stage already exists in Snowflake. Equivalent to SQL ``create stage <name> ...``.

                CreateMode.or_replace: Replace if the stage already exists in Snowflake. Equivalent to SQL
                ``create or replace stage <name> ...``.

                CreateMode.if_not_exists: Do nothing if the stage already exists in Snowflake. Equivalent to SQL
                ``create stage <name> if not exists...``

                Default value is CreateMode.error_if_exists.
        """
        real_mode = CreateMode[mode].value
        self._api.create_stage(
            self.database.name, self.schema.name, stage, create_mode=StrictStr(real_mode), async_req=False
        )
        return StageResource(stage.name, self)

    @api_telemetry
    def iter(
        self,
        *,
        like: Optional[str] = None,
    ) -> Iterator[Stage]:
        """Search ``Stage`` objects from Snowflake.

        Args:
            like: The pattern of the Stage name. Use ``%`` to represent any number of characters and ``?`` for a
                single character.
        """
        stages = self._api.list_stages(
            database=self.database.name, var_schema=self.schema.name, like=like, async_req=False
        )

        return iter(stages)


class StageResource(SchemaObjectReferenceMixin[StageCollection]):
    """Represents a reference to a Snowflake Stage resource."""

    def __init__(self, name: str, collection: StageCollection) -> None:
        self.collection = collection
        self.name = name

    @api_telemetry
    def fetch(self) -> Stage:
        """Fetch the details of a stage."""
        return self.collection._api.fetch_stage(
            self.database.name,
            self.schema.name,
            self.name,
            async_req=False,
        )

    @api_telemetry
    @deprecated("drop")
    def delete(self) -> None:
        """Delete the stage."""
        self.drop()

    @api_telemetry
    def drop(self) -> None:
        """Drop the stage."""
        self.collection._api.delete_stage(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def list_files(
        self,
        *,
        pattern: Optional[str] = None,
    ) -> Iterator[StageFile]:
        """List files in the stage.

        Args:
            pattern: Specifies a regular expression pattern for filtering files from the output.
            The command lists all files in the specified path and applies the regular expression pattern on each
            of the files found.
        """
        files = self.collection._api.list_files(
            self.database.name, self.schema.name, self.name, pattern, async_req=False
        )
        return iter(files)

    @api_telemetry
    @deprecated("put")
    def upload_file(
        self,
        file_path: str,
        stage_folder_path: str,
        *,
        auto_compress: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Upload a file to a stage location.

        Currently only supports uploading files smaller than 1MB to server-side encrypted stages.

        Args:
            file_path: A string representing the location of the file on the client machine to be uploaded.
            stage_folder_path: The stage folder location to be uploaded to, e.g. /folder or /
            auto_compress: Specifies whether Snowflake uses gzip to compress files during upload:
                True: Snowflake compresses the files (if they are not already compressed).

                False: Snowflake does not compress the files.
            overwrite: Specifies whether Snowflake overwrites an existing file with the same name during upload:
                True: An existing file with the same name is overwritten.

                False: An existing file with the same name is not overwritten.

        Raise `APIError` if upload failed
        """
        self.put(file_path, stage_folder_path, auto_compress=auto_compress, overwrite=overwrite)

    @api_telemetry
    @deprecated("get")
    def download_file(self, stage_path: str, file_folder_path: str) -> None:
        """Download a file from a stage location.

        Currently only supports downloading files smaller than 1MB from server-side encrypted stages.

        Args:
            stage_path: The stage location of the file to be downloaded from.
            file_folder_path: A string representing the folder location of the file to be written to.
        """
        self.get(stage_path, file_folder_path)

    @api_telemetry
    def put(
        self,
        local_file_name: Union[str, PathLike],  # type: ignore[type-arg]
        stage_location: str,
        *,
        parallel: int = 4,
        auto_compress: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ) -> None:
        """Upload local files to a path in the stage.

        References: `Snowflake PUT command <https://docs.snowflake.com/en/sql-reference/sql/put.html>`_.

        Args:
            local_file_name: The path to the local files to upload. To match multiple files in the path,
                you can specify the wildcard characters ``*`` and ``?``.
            stage_location: The prefix where you want to upload the files. e.g. /folder or /
            parallel: Specifies the number of threads to use for uploading files. The upload process separates batches
                of data files by size:

                  - Small files (< 64 MB) are staged in parallel as individual files.
                  - Larger files are automatically split into chunks, staged concurrently, and reassembled in the target
                    stage. A single thread can upload multiple chunks.

                Increasing the number of threads can improve performance when uploading large files.
                Supported values: Any integer value from 1 (no parallelism) to 99 (use 99 threads for uploading files).
            auto_compress: Specifies whether Snowflake uses gzip to compress files during upload.
            source_compression: Specifies the method of compression used on already-compressed files that are being
                staged.
                Values can be 'AUTO_DETECT', 'GZIP', 'BZ2', 'BROTLI', 'ZSTD', 'DEFLATE', 'RAW_DEFLATE', 'NONE'.
            overwrite: Specifies whether Snowflake will overwrite an existing file with the same name during upload.

        """
        stage_name = f"@{self.database.name}.{self.schema.name}.{self.name}"
        norm_stage_path = stage_location if stage_location.startswith("/") else f"/{stage_location}"
        self.collection.root.session.file.put(
            fspath(local_file_name),
            f"{stage_name}{norm_stage_path}",
            parallel=parallel,
            auto_compress=auto_compress,
            source_compression=source_compression,
            overwrite=overwrite,
        )

    @api_telemetry
    def get(
        self,
        stage_location: str,
        target_directory: Union[str, PathLike],  # type: ignore[type-arg]
        *,
        parallel: int = 4,
        pattern: Optional[str] = None,
    ) -> None:
        """Download the specified files from a path in the stage to a local directory.

        References: `Snowflake GET command <https://docs.snowflake.com/en/sql-reference/sql/get.html>`_.

        Args:
            stage_location: A directory or filename on a stage, from which you want to download the files.
                e.g. /folder/file_name.txt or /folder
            target_directory: The path to the local directory where the files should be downloaded.
                If ``target_directory`` does not already exist, the method creates the directory.
            parallel: Specifies the number of threads to use for downloading the files.
                The granularity unit for downloading is one file.
                Increasing the number of threads might improve performance when downloading large files.
                Valid values: Any integer value from 1 (no parallelism) to 99 (use 99 threads for downloading files).
            pattern: Specifies a regular expression pattern for filtering files to download.
                The command lists all files in the specified path and applies the regular expression pattern on each of
                the files found.
                Default: ``None`` (all files in the specified stage are downloaded).

        """
        stage_name = f"@{self.database.name}.{self.schema.name}.{self.name}"
        norm_stage_path = stage_location if stage_location.startswith("/") else f"/{stage_location}"
        self.collection.root.session.file.get(
            f"{stage_name}{norm_stage_path}", fspath(target_directory), parallel=parallel, pattern=pattern
        )
