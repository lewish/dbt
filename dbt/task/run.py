from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_types import NodeType
from dbt.node_runners import ModelRunner

import dbt.ui.printer

from dbt.task.base_task import RunnableTask

import shutil
import os


class RunTask(RunnableTask):
    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": [NodeType.Model],
            "tags": []
        }

        # Clean out the target-path/debug directory as we write to files in it with append.
        shutil.rmtree(os.path.join(self.project['target-path'], "debug"), ignore_errors=True)

        results = runner.run(query, ModelRunner)

        if results:
            dbt.ui.printer.print_run_end_messages(results)

        return results
