import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "dev.yaml"
AUTHOR = ("Zoë Développeuse 🌈", "zoe+dev@example.com")


def verifier_source():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    pattern = (
        r"^          # fork-dco-verifier-begin\n"
        r"          python3 - <<'PY'\n"
        r"(.*?)"
        r"^          PY\n"
        r"          # fork-dco-verifier-end$"
    )
    matches = re.findall(pattern, workflow, re.MULTILINE | re.DOTALL)
    if len(matches) != 1:
        raise ValueError("could not isolate the one actual workflow verifier")
    return "\n".join(
        line.removeprefix("          ") for line in matches[0].splitlines()
    ) + "\n"


def wait_source():
    lines = WORKFLOW.read_text(encoding="utf-8").splitlines()
    index = lines.index("      - name: Wait for the other required checks") + 1
    while lines[index] != "        run: |":
        index += 1
    script = []
    for line in lines[index + 1:]:
        if not line.startswith("          "):
            break
        script.append(line.removeprefix("          "))
    return "\n".join(script) + "\n"


class ForkDCOTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.repository = self.directory / "repository"
        self.repository.mkdir()
        self.verifier = self.directory / "verifier.py"
        self.verifier.write_text(verifier_source(), encoding="utf-8")
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", AUTHOR[0])
        self.git("config", "user.email", AUTHOR[1])
        self.git("config", "commit.gpgsign", "false")
        self.base = self.commit("Initial base is deliberately unsigned")

    def git(self, *args, input=None, env=None):
        return subprocess.run(
            ["git", *args], cwd=self.repository, input=input, capture_output=True,
            text=True, encoding="utf-8", check=True, env={**os.environ, **(env or {})},
        ).stdout.strip()

    def commit(self, message, author=None):
        env = {}
        if author:
            env = {"GIT_AUTHOR_NAME": author[0], "GIT_AUTHOR_EMAIL": author[1]}
        self.git("commit", "-q", "--allow-empty", "-F", "-", input=message, env=env)
        return self.git("rev-parse", "HEAD")

    def signed(self, subject, author=AUTHOR):
        return self.commit(f"{subject}\n\nSigned-off-by: {author[0]} <{author[1]}>\n", author)

    def run_verifier(self, head, base=None, cwd=None):
        return subprocess.run(
            [sys.executable, self.verifier], cwd=cwd or self.repository,
            env={**os.environ, "HEAD_SHA": head, "BASE_SHA": self.base if base is None else base},
            text=True, encoding="utf-8", capture_output=True,
        )

    def assert_verified(self, result, count=None, merges=0):
        self.assertEqual(0, result.returncode, result.stderr)
        if count is not None:
            self.assertIn(f"{count} contribution commits", result.stdout)
            self.assertIn(f"{merges} merge commits", result.stdout)

    def assert_rejected(self, result, missing=None):
        self.assertEqual(1, result.returncode, result.stdout)
        if missing:
            self.assertIn(missing, result.stderr)

    def test_signed_contributors_and_unicode_are_accepted(self):
        self.signed("First")
        head = self.commit(
            "Second\n\nMachine: test\n"
            "Signed-off-by: Someone Else <else@example.com>\n"
            "sIgNeD-oFf-bY: ZOË DÉVELOPPEUSE 🌈 <ZOE+DEV@EXAMPLE.COM>\n"
        )
        self.assert_verified(self.run_verifier(head), count=2)

    def test_unsigned_head_or_ancestor_is_rejected(self):
        ancestor = self.commit("Unsigned ancestor")
        head = self.signed("Signed head")
        self.assert_rejected(self.run_verifier(head), ancestor)
        unsigned_head = self.commit("Unsigned head")
        result = self.run_verifier(unsigned_head)
        self.assert_rejected(result, ancestor)
        self.assertIn(unsigned_head, result.stderr)

    def test_only_author_matching_valid_actual_git_trailer_is_accepted(self):
        samples = {
            "wrong-name": (AUTHOR, "Signed-off-by: Other Person <zoe+dev@example.com>"),
            "wrong-email": (AUTHOR, "Signed-off-by: Zoë Développeuse 🌈 <other@example.com>"),
            "committer-only": (("Other Person", "other@example.com"), f"Signed-off-by: {AUTHOR[0]} <{AUTHOR[1]}>"),
            "no-angle-brackets": (AUTHOR, "Signed-off-by: Zoë Développeuse 🌈 zoe+dev@example.com"),
            "signoff-only-in-body": (AUTHOR, f"Signed-off-by: {AUTHOR[0]} <{AUTHOR[1]}>\n\nThis is body prose after the imitated trailer."),
            "unicode-author-mismatch": (AUTHOR, "Signed-off-by: Zoe Developpeuse 🌈 <zoe+dev@example.com>"),
            "author-invalid-email": ((AUTHOR[0], "zoe@example"), f"Signed-off-by: {AUTHOR[0]} <zoe@example>"),
            "author-invalid-dotted-email": ((AUTHOR[0], "zoe..dev@example.com"), f"Signed-off-by: {AUTHOR[0]} <zoe..dev@example.com>"),
            "author-invalid-domain": ((AUTHOR[0], "zoe@-example.com"), f"Signed-off-by: {AUTHOR[0]} <zoe@-example.com>"),
        }
        for key, (author, signoff) in samples.items():
            with self.subTest(key=key):
                self.git("switch", "--detach", self.base)
                head = self.commit(f"{key}\n\n{signoff}\n", author)
                self.assert_rejected(self.run_verifier(head), head)

    def test_genuine_unsigned_merge_is_exempt_but_side_history_is_not(self):
        self.signed("Signed main")
        self.git("switch", "-c", "unsigned-side", self.base)
        bad_side = self.commit("Unsigned side contribution")
        self.signed("Signed side after bad")
        self.git("switch", "main")
        self.git("merge", "--no-ff", "-m", "Unsigned native Git merge", "unsigned-side")
        merge = self.git("rev-parse", "HEAD")
        self.assertEqual(2, len(self.git("show", "-s", "--format=%P", merge).split()))
        result = self.run_verifier(merge)
        self.assert_rejected(result, bad_side)
        self.assertNotIn(merge, result.stderr)

    def test_unsigned_merge_of_only_signed_contributors_is_accepted(self):
        self.signed("Signed main")
        self.git("switch", "-c", "signed-side", self.base)
        self.signed("Signed side")
        self.git("switch", "main")
        self.git("merge", "--no-ff", "-m", "Unsigned native Git merge", "signed-side")
        head = self.git("rev-parse", "HEAD")
        self.assert_verified(self.run_verifier(head), count=2, merges=1)

    def test_only_exact_existing_head_base_and_nonempty_range_are_accepted(self):
        head = self.signed("Signed head")
        self.assert_rejected(self.run_verifier(self.base), "checkout is not")
        self.assert_rejected(self.run_verifier(head, head), "contribution range")
        self.assert_rejected(self.run_verifier("HEAD"), "invalid event commit SHA")
        self.assert_rejected(self.run_verifier(head, "0" * 40), "Unable to verify")
        self.git("switch", "--orphan", "unrelated")
        other = self.signed("Signed but unrelated root")
        self.assert_rejected(self.run_verifier(other), "Unable to verify")

    def test_target_advancements_outside_the_contribution_range_are_not_checked(self):
        advancement = self.commit("Unsigned change added to the target branch")
        self.git("switch", "-c", "feature", self.base)
        head = self.signed("Signed feature")
        self.assert_verified(self.run_verifier(head, advancement), count=1)

    def test_shallow_checkout_is_never_accepted(self):
        head = self.signed("Signed head")
        clone = self.directory / "shallow"
        subprocess.run(
            ["git", "clone", "-q", "--depth", "1", self.repository.as_uri(), clone],
            text=True, capture_output=True, check=True,
        )
        self.assert_rejected(self.run_verifier(head, cwd=clone), "complete checkout is required")

    def test_all_commits_beyond_pull_request_rest_limit_are_checked(self):
        tree = self.git("rev-parse", f"{self.base}^{{tree}}")
        head = self.base
        for index in range(1, 254):
            message = f"Commit {index}\n\nSigned-off-by: {AUTHOR[0]} <{AUTHOR[1]}>\n"
            head = self.git("commit-tree", tree, "-p", head, "-F", "-", input=message)
        self.git("update-ref", "refs/heads/main", head)
        self.assert_verified(self.run_verifier(head), count=253)
        bad = self.commit("Unsigned contribution beyond the REST API limit")
        head = self.signed("Signed head beyond the unsigned contribution")
        self.assert_rejected(self.run_verifier(head), bad)

    def test_real_preflight_prerequisite_selection_and_strict_fork_success(self):
        self.assertIsNotNone(shutil.which("jq"), "the real preflight needs jq")
        directory = self.directory / "bin"
        directory.mkdir()
        gh = directory / "gh"
        gh.write_text('#!/bin/sh\ncat "$DCO_TEST_RUNS"\n', encoding="utf-8")
        gh.chmod(0o755)
        runs = self.directory / "runs.json"
        common = [
            "Linux: Verify use of forbidden licenses", "unit (ubuntu-latest)",
            "unit (macos-latest)", "unit (windows-latest)",
        ]
        cases = [
            ("telepresenceio/telepresence", {"DCO": "success", "DCO (fork)": "skipped"}, 0),
            ("telepresenceio/telepresence", {"DCO": "skipped", "DCO (fork)": "failure"}, 0),
            ("telepresenceio/telepresence", {"DCO": "failure", "DCO (fork)": "success"}, 1),
            ("breland-openai/telepresence", {"DCO (fork)": "success", "DCO verifier tests (fork)": "success"}, 0),
            ("breland-openai/telepresence", {"DCO": "success", "DCO (fork)": "skipped", "DCO verifier tests (fork)": "success"}, 1),
            ("breland-openai/telepresence", {"DCO": "success", "DCO (fork)": "neutral", "DCO verifier tests (fork)": "success"}, 1),
            ("breland-openai/telepresence", {"DCO": "success", "DCO (fork)": "failure", "DCO verifier tests (fork)": "success"}, 1),
            ("breland-openai/telepresence", {"DCO": "success", "DCO (fork)": "success", "DCO verifier tests (fork)": "skipped"}, 1),
        ]
        for repository, states, expected in cases:
            with self.subTest(repository=repository, states=states):
                checks = {**dict.fromkeys(common, "success"), **states}
                runs.write_text(json.dumps([
                    {"name": name, "status": "completed", "conclusion": conclusion}
                    for name, conclusion in checks.items()
                ]), encoding="utf-8")
                result = subprocess.run(
                    ["bash", "-c", wait_source()], text=True, capture_output=True,
                    env={**os.environ, "GITHUB_REPOSITORY": repository, "HEAD_SHA": self.base,
                         "DCO_TEST_RUNS": str(runs), "PATH": f"{directory}{os.pathsep}{os.environ['PATH']}"},
                )
                self.assertEqual(expected, result.returncode, result.stdout + result.stderr)

    def test_real_preflight_requires_native_github_actions_identity_for_fork(self):
        matches = re.findall(r"--jq '([^']+)'", wait_source())
        self.assertEqual(1, len(matches))
        records = []
        for name in ["DCO (fork)", "DCO verifier tests (fork)"]:
            records.extend([
                {"name": name, "app": {"slug": "github-actions"}, "status": "completed", "conclusion": "failure", "started_at": "2026-01-01T00:00:00Z"},
                {"name": name, "app": {"slug": "unrelated-app"}, "status": "completed", "conclusion": "success", "started_at": "2026-01-01T00:01:00Z"},
            ])
        records.append({"name": "DCO", "app": {"slug": "dco"}, "status": "completed", "conclusion": "success", "started_at": "2026-01-01T00:00:00Z"})
        result = subprocess.run(
            ["jq", matches[0]], input=json.dumps({"check_runs": records}),
            text=True, capture_output=True, check=True,
        )
        actual = {check["name"]: check["conclusion"] for check in json.loads(result.stdout)}
        self.assertEqual({"DCO": "success", "DCO (fork)": "failure", "DCO verifier tests (fork)": "failure"}, actual)


if __name__ == "__main__":
    unittest.main(verbosity=2)
