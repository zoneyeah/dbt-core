import os
from datetime import datetime

import agate

from .runnable import ManifestTask

import dbt.exceptions
from dbt.adapters.factory import get_adapter
# from dbt.config.utils import parse_cli_vars
from dbt.contracts.results import MetaArtifact
from dbt.exceptions import InternalException
from dbt.logger import GLOBAL_LOGGER as logger

META_FILENAME = 'meta.json'


class MetaTask(ManifestTask):
    def _get_macro_parts(self):
        macro_name = self.args.type
        if '.' in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        return package_name, f"get_{macro_name}"  # hax

    # def _get_kwargs(self) -> Dict[str, Any]:
    #     return parse_cli_vars(self.args.args)

    def compile_manifest(self) -> None:
        if self.manifest is None:
            raise InternalException('manifest was None in compile_manifest')

    def _run_unsafe(self) -> agate.Table:
        adapter = get_adapter(self.config)

        package_name, macro_name = self._get_macro_parts()
        # macro_kwargs = self._get_kwargs()

        with adapter.connection_named(f'macro_{macro_name}'):
            adapter.clear_transaction()
            res = adapter.execute_macro(
                macro_name,
                project=package_name,
                # kwargs=macro_kwargs,
                manifest=self.manifest
            )

        return res

    def run(self) -> MetaArtifact:
        self._runtime_initialize()
        try:
            res = self._run_unsafe()
        except dbt.exceptions.Exception as exc:
            logger.error(
                'Encountered an error while running operation: {}'
                .format(exc)
            )
            logger.debug('', exc_info=True)
        except Exception as exc:
            logger.error(
                'Encountered an uncaught exception while running operation: {}'
                .format(exc)
            )
            logger.debug('', exc_info=True)
            raise
        else:
            end = datetime.utcnow()
            artifact = MetaArtifact.from_results(
                generated_at=end,
                table=res
            )
            path = os.path.join(self.config.target_path, META_FILENAME)
            artifact.write(path)
            return artifact

    def interpret_results(self, results):
        return True
