# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import

import re

import jinja2
import six

from airflow import conf
from airflow.models import DagBag, TaskInstance
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils import timezone


class UndefinedJinjaVariablesRule(BaseRule):

    title = "Jinja Template Variables cannot be undefined"

    description = """\
Jinja Templates have been updated to the following rule - jinja2.StrictUndefined
With this change a task will fail if it recieves any undefined variables.
"""

    def _check_rendered_content(self, rendered_content):
        """Replicates the logic in BaseOperator.render_template() to
        cover all the cases needed to be checked.
        """
        if isinstance(rendered_content, six.string_types):
            return set(re.findall(r"{{(.*?)}}", rendered_content))

        elif isinstance(rendered_content, (tuple, list, set)):
            debug_error_messages = set()
            for element in rendered_content:
                debug_error_messages.union(self._check_rendered_content(element))
            return debug_error_messages

        elif isinstance(rendered_content, dict):
            debug_error_messages = set()
            for key, value in rendered_content.items():
                debug_error_messages.union(self._check_rendered_content(str(value)))
            return debug_error_messages

    def _render_task_content(self, task, content, context):
        completed_rendering = False
        errors_while_rendering = []
        while not completed_rendering:
            # Catch errors such as {{ object.element }} where
            # object is not defined
            try:
                renderend_content = task.render_template(content, context)
                completed_rendering = True
            except Exception as e:
                undefined_variable = re.sub(" is undefined", "", str(e))
                undefined_variable = re.sub("'", "", undefined_variable)
                context[undefined_variable] = dict()
                message = "Could not find the object '{}'".format(undefined_variable)
                errors_while_rendering.append(message)
        return renderend_content, errors_while_rendering

    def _task_level_(self, task):
        messages = {}
        task_instance = TaskInstance(task=task, execution_date=timezone.utcnow())
        context = task_instance.get_template_context()
        for attr_name in task.template_fields:
            content = getattr(task, attr_name)
            if content:
                rendered_content, errors_while_rendering = self._render_task_content(
                    task, content, context
                )
                debug_error_messages = list(
                    self._check_rendered_content(rendered_content)
                )
                messages[attr_name] = errors_while_rendering + debug_error_messages

        return messages

    def _dag_level_(self, dag):
        dag.template_undefined = jinja2.DebugUndefined
        tasks = dag.tasks
        messages = {}
        for task in tasks:
            error_messages = self._task_level_(task)
            messages[task.task_id] = error_messages
        return messages

    def check(self, dagbag=None):
        if not dagbag:
            dag_folder = conf.get("core", "dags_folder")
            dagbag = DagBag(dag_folder)
        dags = dagbag.dags
        messages = []
        for dag_id, dag in dags.items():
            dag_messages = self._dag_level_(dag)

            for task_id, task_messages in dag_messages.items():
                for attr_name, error_messages in task_messages.items():
                    for error_message in error_messages:
                        message = (
                            "Possible UndefinedJinjaVariable -> DAG: {}, Task: {}, "
                            "Attribute: {}, Error: {}".format(
                                dag_id, task_id, attr_name, error_message.strip()
                            )
                        )
                        messages.append(message)
        return messages
