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
from airflow.upgrade.rules.undefined_jinja_varaibles import UndefinedJinjaVariablesRule
from tests.models import DEFAULT_DATE


class TestConnTypeIsNotNullableRule(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.empty_dir = mkdtemp()

    def setUp(self):

        self.dag = DAG(dag_id="test-undefined-jinja-variables", start_date=DEFAULT_DATE)

        template_command = """
            {% for i in range(5) %}
                echo "{{ params.defined_variable }}"
                echo "{{ execution_date.today }}"
                echo "{{ params.undefined_variable }}"
                echo "{{ foo }}"
            {% endfor %}
            """

        BashOperator(
            task_id="templated_string",
            depends_on_past=False,
            bash_command=template_command,
            params={"defined_variable": "defined_value"},
            dag=self.dag,
        )

        self.dagbag = DagBag(dag_folder=self.empty_dir, include_examples=False)
        self.dagbag.dags[self.dag.dag_id] = self.dag

    def test_check(self):
        rule = UndefinedJinjaVariablesRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        messages = rule.check(self.dagbag)

        expected_messages = [
            "Possible Undefined Jinja Variable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Variable: undefined",
            "Possible Undefined Jinja Variable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Variable: foo",
        ]

        print(messages)
        assert [m for m in messages if m in expected_messages], len(messages) == len(
            expected_messages
        )
