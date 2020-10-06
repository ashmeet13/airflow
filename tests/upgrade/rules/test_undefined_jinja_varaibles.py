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

from tempfile import mkdtemp
from unittest import TestCase

from airflow import DAG
from airflow.models import DagBag
from airflow.operators.bash_operator import BashOperator
from airflow.upgrade.rules.undefined_jinja_varaibles import \
    UndefinedJinjaVariablesRule
from tests.models import DEFAULT_DATE


class TestConnTypeIsNotNullableRule(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.empty_dir = mkdtemp()

    def setUp(self):

        self.invalid_dag = DAG(
            dag_id="test-undefined-jinja-variables", start_date=DEFAULT_DATE
        )
        self.valid_dag = DAG(
            dag_id="test-defined-jinja-variables", start_date=DEFAULT_DATE
        )

        template_command = """
            {% for i in range(5) %}
                echo "{{ params.defined_variable }}"
                echo "{{ execution_date.today }}"
                echo "{{ execution_date.invalid_element }}"
                echo "{{ params.undefined_variable }}"
                echo "{{ foo }}"
            {% endfor %}
            """

        BashOperator(
            task_id="templated_string",
            depends_on_past=False,
            bash_command=template_command,
            env={"undefined_object": "{{ undefined_object.element }}"},
            params={"defined_variable": "defined_value"},
            dag=self.invalid_dag,
        )

        BashOperator(
            task_id="templated_string",
            depends_on_past=False,
            bash_command="echo",
            env={"defined_object": "{{ params.element }}"},
            params={
                "element": "defined_value",
            },
            dag=self.valid_dag,
        )

    def test_invalid_check(self):
        dagbag = DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.dags[self.invalid_dag.dag_id] = self.invalid_dag
        rule = UndefinedJinjaVariablesRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        messages = rule.check(dagbag)

        expected_messages = [
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: env, Error: Could not find the "
            "object 'undefined_object",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: bash_command, Error: no such element: "
            "dict object['undefined_variable']",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: bash_command, Error: no such element: "
            "pendulum.pendulum.Pendulum object['invalid_element']",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: bash_command, Error: foo",
        ]

        assert [m for m in messages if m in expected_messages], len(messages) == len(
            expected_messages
        )

    def test_valid_check(self):
        dagbag = DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.dags[self.valid_dag.dag_id] = self.valid_dag
        rule = UndefinedJinjaVariablesRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        messages = rule.check(dagbag)

        assert len(messages) == 0
