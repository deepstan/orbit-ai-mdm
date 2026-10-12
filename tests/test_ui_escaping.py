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
const els = {};
const ctx = { document: { getElementById: (id) => els[id] || (els[id] = el()) }, fetch: async () => ({ ok: true, json: async () => [] }),
              setInterval() {}, setTimeout() {}, console };
vm.createContext(ctx);
vm.runInContext(script, ctx);
const evil = '<img src=x onerror=alert(1)>';
// Script-level const/let bindings are not properties of the context's global object (function
// declarations are), so reach the helper by evaluating its name inside the same context.
let esc = null;
try { esc = vm.runInContext('esc', ctx); } catch (e) { /* an old UI has no helper; the render checks below still run */ }
// the two long lists, shown a page at a time: drive the real render code with hostile data and read what it wrote
vm.runInContext(`DOMAINS = [{ id: 'customer', fields: ['first_name', 'last_name'] }]; currentDomain = 'customer';
  entitiesLoaded = [{ entity_id: ${JSON.stringify('ent_' + evil)}, attributes: { first_name: ${JSON.stringify(evil)}, last_name: ${JSON.stringify(evil)} }, source_record_ids: [1, 2] }];
  entitiesCursor = 'next-page-cursor'; renderEntities();
  queueLoaded = [{ candidate_id: 'cand_1', match_type: ${JSON.stringify(evil)}, score: 0.9, reasons: [${JSON.stringify(evil)}], contributions: [],
                   record_a: { source_system: ${JSON.stringify(evil)}, attributes: { name: ${JSON.stringify(evil)} } }, record_b: null }];
  queueCursor = 'next-page-cursor'; queueTotal = ${JSON.stringify(evil)}; renderQueue();`, ctx);
const entitiesHtml = els['entities-container'].innerHTML, queueHtml = els['queue'].innerHTML;
const pendingText = els['stat-pending'].textContent, entitiesStat = els['stat-entities'].textContent;
vm.runInContext(`currentDomain = ${JSON.stringify(evil)}; queueLoaded = []; queueCursor = null; queueTotal = 0; renderQueue(); entitiesLoaded = []; renderEntities();`, ctx);
const emptyQueueHtml = els['queue'].innerHTML, emptyEntitiesHtml = els['entities-container'].innerHTML;
console.log(JSON.stringify({
  entitiesHtml, queueHtml, emptyQueueHtml, emptyEntitiesHtml, entitiesStat,
  record: ctx.renderRecord({ source_system: evil, attributes: { ['<b>' + evil]: evil } }),
  candidate: ctx.renderCandidate({ candidate_id: 'cand_1', match_type: evil, score: 0.9, reasons: [evil],
                                   record_a: { source_system: 'crm', attributes: { name: evil } }, record_b: null,
                                   contributions: [{ field: evil, state: evil, similarity: 0.5, points: 0.1 }] }),
  audit: ctx.renderAudit({ candidate_id: 'cand_1', made_by: evil, match_type: evil, score: 0.9, reasons: [evil],
                           contributions: [{ field: evil, state: evil, similarity: 0.5, points: 0.1 }],
                           record_a: { source_system: evil, attributes: { name: evil } }, record_b: null, unmerge: { possible: false, why: evil } }),
  auditCanUnmerge: ctx.renderAudit({ candidate_id: 'cand_2', made_by: 'model', match_type: 'fuzzy', score: 0.9, reasons: [], contributions: [],
                                     record_a: null, record_b: null, unmerge: { possible: true, moves: evil } }),
  auditSummary: ctx.renderAuditSummary({ waiting: evil, target: 0.0005, strata: { rule: { merges: evil, audited: evil, wrong: evil, estimate: 0.1, lower_bound: 0, upper_bound: 0.2 },
                                         promoted: { merges: 1, audited: 1, wrong: 0, estimate: 0.1, lower_bound: 0, upper_bound: 0.2 } },
                                         scorer: { present: true, state: 'rolled_back', reason: evil, promotion_live: false } }),
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
        for name in ("record", "candidate", "audit", "auditCanUnmerge", "auditSummary"):
            self.assertNotIn("<img", out[name], f"{name}: unescaped HTML reached the page")
            self.assertIn("&lt;img", out[name], f"{name}: the data should still be visible, as text")
        self.assertIn("contrib", out["candidate"])  # the per-field table is rendered, and still inert (checked above)
        self.assertIn("contrib", out["audit"])
        self.assertIn("Cannot be unmerged on its own", out["audit"])
        self.assertIn("ROLLED BACK", out["auditSummary"])
        self.assertNotIn("onerror=alert", out["auditCanUnmerge"].replace("&lt;img src=x onerror=alert(1)&gt;", ""))   # the hostile text shows only as escaped text
        for name in ("entitiesHtml", "queueHtml", "emptyQueueHtml", "emptyEntitiesHtml"):   # the paged lists, and their empty states (which print the domain's name)
            self.assertNotIn("<img", out[name], f"{name}: unescaped HTML reached the page")
            self.assertIn("&lt;img", out[name], f"{name}: the data should still be visible, as text")
        self.assertIn("Show more", out["entitiesHtml"])                       # there is a next page: the button is offered
        self.assertIn("Show more", out["queueHtml"])
        self.assertEqual(out["entitiesStat"], "1+")                           # one loaded, and more: not "1 golden record"
        self.assertNotIn("Show more", out["emptyQueueHtml"])
        self.assertEqual(out["esc_null"], "")
        self.assertEqual(out["esc_quote"], "a&quot;b&#39;c&amp;d")


if __name__ == "__main__":
    unittest.main()
