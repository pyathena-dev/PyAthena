# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from pathlib import Path

from cfnlint.decode import decode


def test_template_has_no_retained_storage_or_inbound_ports():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    resources = template["Resources"]
    assert "SecurityGroupIngress" not in resources["SecurityGroup"]["Properties"]
    assert all(r.get("DeletionPolicy") != "Retain" for r in resources.values())
    assert resources["Instance"]["Properties"]["BlockDeviceMappings"][0]["Ebs"][
        "DeleteOnTermination"
    ]
    assert "VersioningConfiguration" not in resources["Bucket"]["Properties"]
    statements = resources["Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    for statement in statements:
        if "SourceBucket" in str(statement["Resource"]):
            assert statement["Action"] in {"s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"}
        if "SourceDatabase" in str(statement["Resource"]):
            assert not any(
                "Delete" in action or "Create" in action for action in statement["Action"]
            )
