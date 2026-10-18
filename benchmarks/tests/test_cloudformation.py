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
    # Conditional results-bucket statements are checked separately.
    for statement in (s for s in statements if "Fn::If" not in s):
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
    assert all(t.startswith(("r7g.", "r8g.", "c8gn.")) for t in families["arm64"])


def test_spot_by_default_keeps_one_instance_type_across_three_or_four_zones():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    resources = template["Resources"]
    assert template["Parameters"]["Purchase"]["Default"] == "spot"
    assert template["Parameters"]["Zones"]["Default"] == 4
    policy = resources["Fleet"]["Properties"]["MixedInstancesPolicy"]
    assert "Overrides" not in policy["LaunchTemplate"]
    distribution = policy["InstancesDistribution"]
    assert distribution["OnDemandBaseCapacity"] == 0
    assert distribution["OnDemandPercentageAboveBaseCapacity"] == {"Fn::If": ["IsSpot", 0, 100]}
    assert distribution["SpotAllocationStrategy"] == "price-capacity-optimized"
    condition, four, three = resources["Fleet"]["Properties"]["VPCZoneIdentifier"]["Fn::If"]
    assert condition == "UseFourZones"
    for subnets, expected in ((four, [0, 1, 2, 3]), (three, [0, 1, 2])):
        names = [s["Ref"] for s in subnets]
        zones = [resources[n]["Properties"]["AvailabilityZone"]["Fn::Select"][0] for n in names]
        assert sorted(zones) == expected
    assert resources["Subnet4"]["Condition"] == "UseFourZones"
    assert resources["RouteAssociation4"]["Condition"] == "UseFourZones"
    # Hosts launch after every zone's route exists, including the conditional fourth one.
    assert {"RouteAssociation", "RouteAssociation2", "RouteAssociation3"} <= set(
        resources["Fleet"]["DependsOn"]
    )
    [tag] = resources["Fleet"]["Properties"]["Tags"]
    assert tag["Value"]["Fn::If"][:2] == ["UseFourZones", {"Ref": "RouteAssociation4"}]
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
    statements = [s for s in statements if "Fn::If" not in s]
    scoped = [s for s in statements if "autoscaling" in str(s["Action"]) and "Condition" in s]
    unscoped = [s for s in statements if "autoscaling" in str(s["Action"]) and "Condition" not in s]
    # Only the read-only lookup of the host's group is unconditional.
    assert [s["Action"] for s in unscoped] == ["autoscaling:DescribeAutoScalingInstances"]
    [statement] = scoped
    assert statement["Action"] == [
        "autoscaling:SuspendProcesses",
        "autoscaling:TerminateInstanceInAutoScalingGroup",
    ]
    assert statement["Condition"] == {
        "StringEquals": {
            "autoscaling:ResourceTag/aws:cloudformation:stack-id": {"Ref": "AWS::StackId"}
        }
    }


def test_results_location_is_scoped_to_the_stack_prefix():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    statements = template["Resources"]["Role"]["Properties"]["Policies"][0]["PolicyDocument"][
        "Statement"
    ]
    results = [s["Fn::If"] for s in statements if "Fn::If" in s]
    assert results
    assert all(r[0] == "HasResultsBucket" for r in results)
    assert all(r[2] == {"Ref": "AWS::NoValue"} for r in results)
    for _, statement, _ in results:
        if "s3:PutObject" in statement["Action"]:
            assert statement["Resource"]["Fn::Sub"].endswith(
                ":s3:::${ResultsBucket}/${ResultsPrefix}/${AWS::StackName}/*"
            )
        else:
            # Listing and bucket metadata only; object access stays below the prefix.
            assert statement["Action"] == [
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
            ]
            assert statement["Resource"] == {
                "Fn::Sub": "arn:${AWS::Partition}:s3:::${ResultsBucket}"
            }
            assert "Condition" not in statement
    condition, results_bucket, scratch = template["Outputs"]["Results"]["Value"]["Fn::If"]
    assert condition == "HasResultsBucket"
    assert results_bucket == {
        "Fn::Sub": "s3://${ResultsBucket}/${ResultsPrefix}/${AWS::StackName}/"
    }
    assert scratch == {"Fn::Sub": "s3://${Bucket}/"}


def test_hosts_start_the_queue_worker_and_expose_their_tags():
    template, errors = decode(
        str(Path(__file__).resolve().parents[1] / "cloudformation/benchmark.yaml")
    )
    assert not errors
    launch = template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]
    assert launch["MetadataOptions"]["InstanceMetadataTags"] == "enabled"
    user_data, variables = launch["UserData"]["Fn::Base64"]["Fn::Sub"]
    assert "if [ -n '${QueueName}' ]; then" in user_data
    # The worker reads the results location from user data, before the stack's outputs exist.
    assert (
        "ExecStart=/opt/pyathena/.venv/bin/python -m pyathena_bench worker --results ${Results}"
        " --name ${QueueName} --api-slots ${ApiSlots} --wait --terminate-host"
    ) in user_data
    assert (
        "printf '%s\\n' '${Results}' > /opt/pyathena/benchmarks/results-location.txt" in user_data
    )
    assert variables["Results"] == template["Outputs"]["Results"]["Value"]
    assert "systemctl enable --now pyathena-bench-worker.service" in user_data
    # Jobs of the polars2 environment run the interpreter beside the worker's environment.
    assert (
        "UV_PROJECT_ENVIRONMENT=/opt/pyathena/.venv-polars2 uv sync --directory "
        "/opt/pyathena/benchmarks --locked --no-default-groups --group polars2"
    ) in user_data
    assert template["Parameters"]["QueueName"]["Default"] == ""
