from dbt.clients.system import write_json, read_json
from dbt.contracts.graph.manifest import Manifest


manifest_file_name = 'manifest.json'


class ManifestClient:
    """
    Utility class for reading and writing the Manifest to file.
    """

    @classmethod
    def read(cls, file_path):
        """
        Read a json file and generate a valid Manifest.
        """
        return Manifest.deserialize(read_json(file_path))

    @classmethod
    def write(cls, file_path, manifest):
        """
        Write the manifest to a json file on disk.

        manifest should be a Manifest.
        """
        write_json(file_path, manifest.serialize())
