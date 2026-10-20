"""
Things that are being retired say so, once, where they are used (docs/legacy-engines.md, docs/agent-runner.md). Nothing is removed yet: the old engine stays as a rollback until the kernel has
been proven on PostgreSQL, and the old LLM adjudication stays until the agent runner has shadow-mode results to compare with.

  * MDM_ENGINE=legacy warns, ONCE per process; the default engine does not
  * ENABLE_LLM_ADJUDICATION warns, once, only when it is really in use; and the model it asks for is configurable (MDM_LLM_MODEL), because a hard-coded name is a guess about an account
Run with:  python3 -m unittest tests.test_deprecations -v
"""
import json
import os
import sys
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import kernel, llm_adjudication  # noqa: E402


class TestTheOldEngineSaysItIsGoing(unittest.TestCase):
    def setUp(self):
        kernel._warned_legacy = False

    def test_it_warns_once_per_process_that_it_is_deprecated_and_where_to_read_more(self):
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "legacy"}):
            with self.assertLogs("mdm.kernel", level="WARNING") as seen:
                self.assertEqual(kernel.engine(), "legacy")
            self.assertIn("DEPRECATED", seen.output[0])
            self.assertIn("docs/legacy-engines.md", seen.output[0])
            with self.assertNoLogs("mdm.kernel", level="WARNING"):                                   # not at every record
                kernel.engine()
                kernel.engine()

    def test_the_default_engine_says_nothing(self):
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "kernel"}):
            with self.assertNoLogs("mdm.kernel", level="WARNING"):
                self.assertEqual(kernel.engine(), "kernel")

    def test_a_bad_value_is_still_refused_not_warned_about(self):
        with mock.patch.dict(os.environ, {"MDM_ENGINE": "nonsense"}):
            with self.assertRaises(Exception):
                kernel.engine()


class TestTheOldAdjudicationSaysItIsGoing(unittest.TestCase):
    def setUp(self):
        llm_adjudication._warned = False

    def test_using_it_warns_once_and_names_its_replacement(self):
        with mock.patch.dict(os.environ, {"ENABLE_LLM_ADJUDICATION": "true", "ANTHROPIC_API_KEY": "k"}):
            with self.assertLogs("mdm.llm_adjudication", level="WARNING") as seen:
                self.assertTrue(llm_adjudication.is_enabled())
            self.assertIn("DEPRECATED", seen.output[0])
            self.assertIn("agent_runner.py", seen.output[0])
            with self.assertNoLogs("mdm.llm_adjudication", level="WARNING"):
                llm_adjudication.is_enabled()

    def test_not_using_it_says_nothing(self):
        with mock.patch.dict(os.environ, {"ENABLE_LLM_ADJUDICATION": "", "ANTHROPIC_API_KEY": "k"}):
            with self.assertNoLogs("mdm.llm_adjudication", level="WARNING"):
                self.assertFalse(llm_adjudication.is_enabled())
        with mock.patch.dict(os.environ, {"ENABLE_LLM_ADJUDICATION": "true", "ANTHROPIC_API_KEY": ""}):
            with self.assertNoLogs("mdm.llm_adjudication", level="WARNING"):                            # switched on but unusable: not 'in use'
                self.assertFalse(llm_adjudication.is_enabled())

    def test_the_model_is_whatever_the_operator_set_and_otherwise_the_old_default(self):
        with mock.patch.dict(os.environ, {"MDM_LLM_MODEL": ""}):
            self.assertEqual(llm_adjudication.model_name(), llm_adjudication.MODEL)
        with mock.patch.dict(os.environ, {"MDM_LLM_MODEL": "my-model-9"}):
            self.assertEqual(llm_adjudication.model_name(), "my-model-9")

    def test_the_request_really_asks_for_the_configured_model(self):
        asked = []

        def offline(req, timeout=None):
            asked.append(json.loads(req.data))
            raise llm_adjudication.urllib.error.URLError("offline")                                      # it fails safe, as it always did
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "k", "MDM_LLM_MODEL": "my-model-9"}), mock.patch.object(llm_adjudication.urllib.request, "urlopen", offline):
            self.assertIsNone(llm_adjudication.adjudicate({"first_name": "Ann"}, {"first_name": "Anne"}, ["similar"]))
        self.assertEqual(asked[0]["model"], "my-model-9")


if __name__ == "__main__":
    unittest.main()
