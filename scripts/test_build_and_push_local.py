import subprocess
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

import build_and_push_local as build


class LocalBuildTests(unittest.TestCase):
    def test_publish_builds_both_architectures(self):
        command = build.build_command("registry/cloudbreak:tag", False, False)
        self.assertIn("linux/arm64,linux/amd64", command)
        self.assertNotIn("--build-arg", command)
        self.assertIn("--push", command)
        self.assertNotIn("--load", command)

    @patch.object(build.platform, "machine", return_value="arm64")
    def test_local_build_loads_only_one_architecture(self, _):
        command = build.build_command("registry/cloudbreak:tag", False, True)
        self.assertIn("linux/arm64", command)
        self.assertIn("--load", command)
        self.assertNotIn("--push", command)

    @patch.object(build, "run")
    def test_wrong_aws_account_stops_before_ecr_mutation(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, stdout="111111111111\n")
        with self.assertRaisesRegex(ValueError, "select a profile/role"):
            build.setup_ecr("829210487188", "us-east-1", "cloudbreak", "registry")
        self.assertEqual(run.call_count, 1)

    @patch.object(build, "run")
    def test_missing_repository_is_created_before_login(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="829210487188\n"),
            subprocess.CompletedProcess([], 1, stderr="RepositoryNotFoundException"),
            subprocess.CompletedProcess([], 0),
            subprocess.CompletedProcess([], 0, stdout="secret-password\n"),
            subprocess.CompletedProcess([], 0),
        ]
        build.setup_ecr("829210487188", "us-east-1", "cloudbreak", "registry")
        self.assertIn("create-repository", run.call_args_list[2].args[0])
        self.assertEqual(run.call_args_list[4].kwargs["input"], "secret-password")
        self.assertNotIn("secret-password", run.call_args_list[4].args[0])

    @patch.object(build, "run")
    def test_ecr_permission_error_does_not_create_repository(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="829210487188\n"),
            subprocess.CompletedProcess([], 1, stderr="AccessDeniedException"),
        ]
        with self.assertRaisesRegex(RuntimeError, "AccessDeniedException"):
            build.setup_ecr("829210487188", "us-east-1", "cloudbreak", "registry")
        self.assertEqual(run.call_count, 2)

    @patch.object(build, "setup_ecr")
    @patch.object(build, "run")
    @patch.object(build.sys, "argv", ["build_and_push_local.py", "--dry-run"])
    def test_dry_run_never_contacts_aws(self, run, setup_ecr):
        run.return_value = subprocess.CompletedProcess([], 0, stdout="abc123\n")
        with redirect_stdout(StringIO()):
            build.main()
        setup_ecr.assert_not_called()
        self.assertIn("--load", run.call_args.args[0])

    @patch.object(build, "run")
    @patch.object(build.sys, "argv", ["build_and_push_local.py", "--plan"])
    def test_plan_only_reads_git(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, stdout="abc123\n")
        with redirect_stdout(StringIO()) as output:
            build.main()
        self.assertEqual(run.call_count, 3)
        self.assertTrue(all(call.args[0][0] == "git" for call in run.call_args_list))
        self.assertIn("--push", output.getvalue())


if __name__ == "__main__":
    unittest.main()
