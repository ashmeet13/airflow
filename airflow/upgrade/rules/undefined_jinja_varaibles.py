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

from airflow.models import DagBag, TaskInstance
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils import timezone


class UndefinedJinjaVariablesRule(BaseRule):

    title = "Jinja Template Variables cannot be undefined"

    description = """\
Jinja Templates have been updated to the following rule - jinja2.StrictUndefined
With this change a task will fail if it recieves any undefined variables.
"""

    def check_rendered_content(self, rendered_content):
        if isinstance(rendered_content, six.string_types):
            return set(re.findall(r"{{(.*?)}}", rendered_content))

        elif isinstance(rendered_content, (tuple, list, set)):
            parsed_templates = set()
            for element in rendered_content:
                parsed_templates.union(self.check_rendered_content(element))
            return parsed_templates

        elif isinstance(rendered_content, dict):
            parsed_templates = set()
            for key, value in rendered_content.items():
                parsed_templates.union(self.check_rendered_content(str(value)))
            return parsed_templates

    def check(self, dagbag=DagBag()):
        dags = dagbag.dags
        messages = []
        for dag_id, dag in dags.items():
            bracket_pattern = r"\[(.*?)\]"
            dag.template_undefined = jinja2.DebugUndefined
            for task in dag.tasks:
                task_instance = TaskInstance(
                    task=task, execution_date=timezone.utcnow()
                )
                template_context = task_instance.get_template_context()

                rendered_content_collection = []

                for attr_name in task.template_fields:
                    content = getattr(task, attr_name)
                    if content:
                        rendered_content_collection.append(
                            task.render_template(content, template_context)
                        )

                for rendered_content in rendered_content_collection:
                    undefined_variables = self.check_rendered_content(rendered_content)
                    for undefined_variable in undefined_variables:
                        result = re.findall(bracket_pattern, undefined_variable)
                        if result:
                            undefined_variable = result[0].strip("'")
                        new_msg = (
                            "Possible Undefined Jinja Variable -> DAG: {}, Task: {}, "
                            "Variable: {}".format(
                                dag_id, task.task_id, undefined_variable.strip()
                            )
                        )
                        messages.append(new_msg)
        return messages
