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
    launch = resources["LaunchTemplate"]["Properties"]["LaunchTemplateData"]
    assert launch["BlockDeviceMappings"][0]["Ebs"]["DeleteOnTermination"]
    assert launch["MetadataOptions"]["HttpTokens"] == "required"
    assert "VersioningConfiguration" not in resources["Bucket"]["Properties"]
    statements = resources["Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    for statement in statements:
        if "SourceBucket" in str(statement["Resource"]):
            assert statement["Action"] in {"s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"}
        if "SourceDatabase" in str(statement["Resource"]):
            assert not any(
                "Delete" in action or "Create" in action for action in statement["Action"]
            )


def test_fleet_size_controls_identical_hosts_and_signals():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    fleet = template["Resources"]["Fleet"]
    assert fleet["Type"] == "AWS::AutoScaling::AutoScalingGroup"
    for key in ("MaxSize", "DesiredCapacity"):
        assert fleet["Properties"][key] == {"Ref": "FleetSize"}
    # A finished worker lowers the desired capacity when it terminates its host.
    assert fleet["Properties"]["MinSize"] == 0
    assert fleet["CreationPolicy"]["ResourceSignal"]["Count"] == {"Ref": "FleetSize"}
    assert template["Parameters"]["FleetSize"]["Default"] == 1
    assert "--resource Fleet" in str(
        template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]["UserData"]
    )
    assert "AutoScalingGroup" in template["Outputs"]


def test_architecture_selects_the_image_and_rules_match_instance_families():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    launch = template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]
    assert launch["ImageId"] == {"Fn::If": ["IsArm64", {"Ref": "Arm64ImageId"}, {"Ref": "ImageId"}]}
    assert "arm64" in template["Parameters"]["Arm64ImageId"]["Default"]
    assert "x86_64" in template["Parameters"]["ImageId"]["Default"]
    allowed = set(template["Parameters"]["InstanceType"]["AllowedValues"])
    families = {}
    for name, architecture in (("X86InstanceType", "x86_64"), ("Arm64InstanceType", "arm64")):
        rule = template["Rules"][name]
        assert rule["RuleCondition"] == {"Fn::Equals": [{"Ref": "Architecture"}, architecture]}
        families[architecture] = set(rule["Assertions"][0]["Assert"]["Fn::Contains"][0])
    assert families["x86_64"] | families["arm64"] == allowed
    assert not families["x86_64"] & families["arm64"]
    assert all(t.startswith("r7i.") for t in families["x86_64"])
    assert all(t.startswith(("r7g.", "r8g.")) for t in families["arm64"])


def test_purchase_option_keeps_one_instance_type_across_three_zones():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    resources = template["Resources"]
    assert template["Parameters"]["Purchase"]["Default"] == "on-demand"
    policy = resources["Fleet"]["Properties"]["MixedInstancesPolicy"]
    assert "Overrides" not in policy["LaunchTemplate"]
    distribution = policy["InstancesDistribution"]
    assert distribution["OnDemandBaseCapacity"] == 0
    assert distribution["OnDemandPercentageAboveBaseCapacity"] == {"Fn::If": ["IsSpot", 0, 100]}
    assert distribution["SpotAllocationStrategy"] == "price-capacity-optimized"
    subnets = resources["Fleet"]["Properties"]["VPCZoneIdentifier"]
    names = [s["Ref"] for s in subnets]
    zones = [resources[n]["Properties"]["AvailabilityZone"]["Fn::Select"][0] for n in names]
    assert sorted(zones) == [0, 1, 2]
    interface = resources["LaunchTemplate"]["Properties"]["LaunchTemplateData"][
        "NetworkInterfaces"
    ][0]
    assert "SubnetId" not in interface


def test_hosts_can_terminate_instances_only_in_their_own_stack():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    statements = template["Resources"]["Role"]["Properties"]["Policies"][0]["PolicyDocument"][
        "Statement"
    ]
    [statement] = [s for s in statements if "autoscaling" in str(s["Action"])]
    assert statement["Action"] == "autoscaling:TerminateInstanceInAutoScalingGroup"
    assert statement["Condition"] == {
        "StringEquals": {
            "autoscaling:ResourceTag/aws:cloudformation:stack-id": {"Ref": "AWS::StackId"}
        }
    }
