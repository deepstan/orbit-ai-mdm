"""
The stewardship console renders data that came from source systems, so it must escape it.
Runs the REAL script from static/index.html in Node (with a stub DOM) against hostile input.
Skipped if Node isn't installed.
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

HTML = os.path.join(os.path.dirname(__file__), "..", "static", "index.html")

HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const script = fs.readFileSync(process.argv[2], 'utf8').split('<script>')[1].split('</script>')[0];
const el = () => ({ innerHTML: '', textContent: '', classList: { add() {}, remove() {} } });
const ctx = { document: { getElementById: () => el() }, fetch: async () => ({ ok: true, json: async () => [] }),
              setInterval() {}, setTimeout() {}, console };
vm.createContext(ctx);
vm.runInContext(script, ctx);
const evil = '<img src=x onerror=alert(1)>';
// Script-level const/let bindings are not properties of the context's global object (function
// declarations are), so reach the helper by evaluating its name inside the same context.
let esc = null;
try { esc = vm.runInContext('esc', ctx); } catch (e) { /* an old UI has no helper; the render checks below still run */ }
console.log(JSON.stringify({
  record: ctx.renderRecord({ source_system: evil, attributes: { ['<b>' + evil]: evil } }),
  candidate: ctx.renderCandidate({ candidate_id: 'cand_1', match_type: evil, score: 0.9, reasons: [evil],
                                   record_a: { source_system: 'crm', attributes: { name: evil } }, record_b: null }),
  esc_null: esc ? esc(null) : null, esc_quote: esc ? esc('a"b\'c&d') : null,
}));
"""


@unittest.skipUnless(shutil.which("node"), "Node.js not installed")
class TestUiEscaping(unittest.TestCase):
    def test_hostile_data_is_rendered_inert(self):
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(HARNESS)
        self.addCleanup(os.remove, f.name)
        run = subprocess.run(["node", f.name, HTML], capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        out = json.loads(run.stdout)
        for name in ("record", "candidate"):
            self.assertNotIn("<img", out[name], f"{name}: unescaped HTML reached the page")
            self.assertIn("&lt;img", out[name], f"{name}: the data should still be visible, as text")
        self.assertEqual(out["esc_null"], "")
        self.assertEqual(out["esc_quote"], "a&quot;b&#39;c&amp;d")


if __name__ == "__main__":
    unittest.main()
