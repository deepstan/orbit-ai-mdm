"""
The ASSISTANT'S SUGGESTION in the stewardship console (static/index.html), tested by running the REAL console script in Node against a stub page and a fake API.

What matters, and is tested:
  * CLOSED BY DEFAULT, AND EMPTY: until a steward opens a suggestion the page holds none of what it says (only that one exists). "Closed" must mean "not seen", because whether the
    steward saw it is sent with the decision, and that is what lets agreement with the assistant be measured on the decisions it did not influence.
  * THE DECISION REPORTS IT: suggestion_viewed is true if the steward opened the panel (even if they closed it again), false if there was one and they did not, and left out if there was none
  * IT NEVER NUDGES: merge and not-a-match look the same; there is no accept control; the two real buttons are identical whatever the suggestion says
  * UNTRUSTED TEXT: what a model wrote goes through esc() like any other data
  * IT SURVIVES THE 8-SECOND REFRESH (an open panel stays open and is not fetched again) and does not loop
  * A FAILING API is a normal state, not an error

Skipped if Node isn't installed.   Run with:  python3 -m unittest tests.test_ui_suggestions -v
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
const scenario = process.argv[3], input = JSON.parse(process.argv[4] || '{}');
const calls = [];
const routes = (input.routes || []).map(r => ({ re: new RegExp(r.match), body: r.body, status: r.status }));
const el = () => ({ innerHTML: '', textContent: '', classList: { add() {}, remove() {} } });
const els = {};
const ctx = { document: { getElementById: (id) => els[id] || (els[id] = el()) },
              fetch: async (url, opts) => {
                calls.push({ url, method: (opts && opts.method) || 'GET', body: opts && opts.body ? JSON.parse(opts.body) : null });
                const r = routes.find(r => r.re.test(url));
                if (r && r.status) return { ok: false, status: r.status, text: async () => 'error' };
                return { ok: true, json: async () => (r ? r.body : []) };
              },
              setInterval() {}, setTimeout() {}, console };
vm.createContext(ctx);
vm.runInContext(script, ctx);
const run = (code) => vm.runInContext(code, ctx);
run(`currentDomain = 'customer';`);
const candidate = (id) => ({ candidate_id: id || 'cand_1', match_type: 'fuzzy', score: 0.8, reasons: ['same email'], contributions: [],
                             record_a: { source_system: 'crm', attributes: { name: 'Ann' } }, record_b: { source_system: 'crm', attributes: { name: 'Anne' } } });
const opinion = (over) => Object.assign({ recommendation: 'confirm', confidence: 0.8, reasons: ['SECRET-REASON same person'], agent: 'agent:runner-1', model: 'model-x', prompt_version: 'steward-v1',
                                           created_at: '2026-10-17T12:00:00+00:00' }, over || {});
const card = (id) => run(`renderCandidate(${JSON.stringify(candidate(id))})`);
const give = (id, ops) => run(`suggestionHints.add(${JSON.stringify(id)}); suggestionsLoaded[${JSON.stringify(id)}] = ${JSON.stringify(ops)};`);
const open = (id) => run(`suggestionsOpen.add(${JSON.stringify(id)});`);
const sets = () => JSON.parse(run(`JSON.stringify({ hints: [...suggestionHints], open: [...suggestionsOpen], viewed: [...suggestionsViewed], loaded: Object.keys(suggestionsLoaded), errors: [...suggestionErrors] })`));
const actionsOf = (html) => html.slice(html.indexOf('<div class="actions">'));

const S = {
  hostile: async () => {
    const evil = '<img src=x onerror=alert(1)>';
    give('cand_1', [opinion({ recommendation: evil, reasons: [evil], agent: evil, model: evil, prompt_version: evil, created_at: evil, confidence: evil })]); open('cand_1');
    give('cand_2', [opinion({ reasons: [evil] })]); open('cand_2'); run(`suggestionStats = { decided: ${JSON.stringify(evil)}, agreed: ${JSON.stringify(evil)} };`);
    return { one: card('cand_1'), two: card('cand_2') };
  },
  closed: async () => {
    give('cand_1', [opinion()]);                                      // loaded earlier (say it was opened once) but closed now
    const closedLoaded = card('cand_1');
    run(`suggestionsLoaded = {};`);
    const closedUnread = card('cand_1');
    run(`suggestionHints = new Set();`);
    return { closedLoaded, closedUnread, none: card('cand_1') };
  },
  open_flow: async () => {
    run(`suggestionHints.add('cand_1');`);
    const before = card('cand_1');
    const pending = run(`onSuggestionToggle({ dataset: { id: 'cand_1' }, open: true })`);
    const whileLoading = run(`renderSuggestion('cand_1')`);
    await pending;
    const loaded = card('cand_1');
    await run(`onSuggestionToggle({ dataset: { id: 'cand_1' }, open: true })`);       // a browser may fire toggle again for a panel drawn open: it must do nothing more
    return { before, whileLoading, loaded, calls: calls.map(c => c.url), state: sets() };
  },
  close_keeps_viewed: async () => {
    run(`suggestionHints.add('cand_1');`);
    await run(`onSuggestionToggle({ dataset: { id: 'cand_1' }, open: true })`);
    await run(`onSuggestionToggle({ dataset: { id: 'cand_1' }, open: false })`);
    return { state: sets(), html: card('cand_1') };
  },
  decisions: async () => {
    run(`suggestionHints.add('cand_a'); suggestionHints.add('cand_b');`);
    await run(`onSuggestionToggle({ dataset: { id: 'cand_a' }, open: true })`);        // opened
    await run(`resolve('cand_a', 'confirm')`);
    await run(`resolve('cand_b', 'reject')`);                                         // there was one; never opened
    await run(`resolve('cand_c', 'confirm')`);                                        // there was none
    const posts = calls.filter(c => c.method === 'POST').map(c => ({ url: c.url, body: c.body }));
    return { posts, state: sets() };
  },
  opened_then_closed_then_decided: async () => {
    run(`suggestionHints.add('cand_a');`);
    await run(`onSuggestionToggle({ dataset: { id: 'cand_a' }, open: true })`);
    await run(`onSuggestionToggle({ dataset: { id: 'cand_a' }, open: false })`);
    await run(`resolve('cand_a', 'confirm')`);
    return { posts: calls.filter(c => c.method === 'POST').map(c => c.body) };
  },
  refresh: async () => {
    run(`queueLoaded = [${JSON.stringify(candidate('cand_1'))}, ${JSON.stringify(candidate('cand_2'))}]; queueTotal = 2;`);
    give('cand_1', [opinion()]); give('cand_2', [opinion()]); open('cand_1');
    run(`renderQueue();`);
    return { html: els['queue'].innerHTML };
  },
  neutral: async () => {
    give('cand_1', [opinion({ recommendation: 'confirm' })]); open('cand_1');
    const confirm = card('cand_1');
    give('cand_1', [opinion({ recommendation: 'reject' })]);
    const reject = card('cand_1');
    give('cand_1', [opinion({ recommendation: 'needs_human' })]);
    const unsure = card('cand_1');
    return { confirm, reject, unsure };
  },
  hints: async () => {
    run(`suggestionHints = new Set();`);                            // start from knowing nothing (the page's own startup refresh has already learned them)
    let renders = 0; ctx.renderQueue = () => { renders++; };
    await run(`refreshSuggestionHints()`);
    const first = renders;
    await run(`refreshSuggestionHints()`);
    return { first, second: renders - first, hints: sets().hints, urls: calls.map(c => c.url) };
  },
  hints_fail: async () => {
    run(`suggestionHints.add('cand_old');`);
    await run(`refreshSuggestionHints()`);
    return { state: sets(), html: card('cand_1') };
  },
  load_fail: async () => {
    run(`suggestionHints.add('cand_1');`);
    await run(`onSuggestionToggle({ dataset: { id: 'cand_1' }, open: true })`);
    return { html: card('cand_1'), state: sets() };
  },
  stats: async () => {
    give('cand_1', [opinion()]); open('cand_1');
    const out = {};
    for (const [name, st] of Object.entries(input.stats)) { run(`suggestionStats = ${JSON.stringify(st)};`); out[name] = card('cand_1'); }
    return out;
  },
  confidence: async () => {
    const out = {};
    for (const c of input.values) { give('cand_1', [opinion({ confidence: c })]); open('cand_1'); out[String(c)] = card('cand_1'); }
    return out;
  },
  switch_domain: async () => {
    give('cand_1', [opinion()]); open('cand_1'); run(`suggestionsViewed.add('cand_1'); suggestionStats = { decided: 1, agreed: 1 };`);
    run(`switchDomain('product')`);
    return { state: sets(), stats: run(`suggestionStats`) };
  },
};
// The script starts its own first refresh as the page loads (as in a browser). Let it finish, and forget its requests, so a scenario sees only what it does itself.
const settle = async () => { for (let i = 0; i < 30; i++) await new Promise(r => setImmediate(r)); calls.length = 0; };
settle().then(() => S[scenario]()).then(out => console.log(JSON.stringify(out))).catch(e => { console.log(JSON.stringify({ crashed: String(e && e.stack || e) })); process.exit(1); });
"""


_DIR = tempfile.mkdtemp()
_FILE = os.path.join(_DIR, "harness.js")
open(_FILE, "w").write(HARNESS)


def tearDownModule():
    shutil.rmtree(_DIR, ignore_errors=True)


def run(scenario, **input):
    done = subprocess.run(["node", _FILE, HTML, scenario, json.dumps(input)], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr
    return json.loads(done.stdout)


@unittest.skipUnless(shutil.which("node"), "Node.js not installed")
class TestTheSuggestionPanel(unittest.TestCase):
    def test_closed_by_default_and_the_text_is_not_in_the_page_until_it_is_opened(self):
        r = run("closed")
        for name in ("closedLoaded", "closedUnread"):
            self.assertIn('<details class="suggest" data-id="cand_1"', r[name], name)
            self.assertNotIn('<details class="suggest" data-id="cand_1" open', r[name], name)
            self.assertNotIn("SECRET-REASON", r[name], name)                         # not hidden by CSS: not there at all
            self.assertNotIn("agent:runner-1", r[name], name)
            self.assertNotIn("80% sure", r[name], name)
            self.assertIn("available", r[name], name)                                # the steward is told one exists
        self.assertNotIn("suggest", r["none"].replace("suggestion", ""))             # no suggestion, no panel at all
        self.assertNotIn('class="suggest"', r["none"])

    def test_opening_it_loads_it_once_shows_it_and_counts_as_seen(self):
        r = run("open_flow", routes=[{"match": "/recommendations/stats", "body": {"decided": 40, "agreed": 31}},
                                     {"match": "/review-queue/cand_1/recommendations", "body": {"items": [{"recommendation": "confirm", "confidence": 0.8, "reasons": ["SECRET-REASON same person"],
                                                                                                          "agent": "agent:runner-1", "model": "model-x", "prompt_version": "steward-v1",
                                                                                                          "created_at": "2026-10-17T12:00:00+00:00"}]}}])
        self.assertNotIn("SECRET-REASON", r["before"])
        self.assertIn("Loading", r["whileLoading"])
        self.assertIn("SECRET-REASON same person", r["loaded"])
        self.assertIn("Same entity: merge", r["loaded"])
        self.assertIn("80% sure", r["loaded"])
        self.assertIn("agent:runner-1", r["loaded"])
        self.assertIn("model-x", r["loaded"])
        self.assertIn("steward-v1", r["loaded"])
        self.assertIn("It changed nothing and it can be wrong. You decide.", r["loaded"])
        self.assertIn("31 of 40 decided cases", r["loaded"])
        self.assertNotIn("too few", r["loaded"])
        self.assertEqual(sum(1 for u in r["calls"] if u.endswith("/review-queue/cand_1/recommendations")), 1)       # fetched once, though toggled twice
        self.assertEqual((r["state"]["open"], r["state"]["viewed"]), (["cand_1"], ["cand_1"]))

    def test_what_a_model_wrote_is_escaped_like_any_other_untrusted_data(self):
        r = run("hostile")
        for name, html in r.items():
            self.assertNotIn("<img", html, name)
            self.assertNotIn("onerror=alert(1)>", html.replace("&lt;img src=x onerror=alert(1)&gt;", ""), name)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", r["one"])                    # shown, as text
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", r["two"])

    def test_it_looks_the_same_whatever_it_says_and_the_real_buttons_do_not_change(self):
        r = run("neutral")
        verdict = lambda h: h[h.index('<span class="verdict'):h.index("</span>", h.index('<span class="verdict'))]
        self.assertEqual(verdict(r["confirm"]).replace("Same entity: merge", ""), verdict(r["reject"]).replace("Not a match", ""))     # same classes: no green for merge, no red for not-a-match
        self.assertIn("unsure", verdict(r["unsure"]))
        self.assertIn("Can&#39;t tell: needs a person", r["unsure"])                          # shown as text: even this apostrophe goes through esc()
        actions = lambda h: h[h.index('<div class="actions">'):]
        self.assertEqual(actions(r["confirm"]), actions(r["reject"]))                      # the two decision buttons are byte for byte the same
        self.assertEqual(actions(r["confirm"]), actions(r["unsure"]))
        for html in r.values():
            self.assertNotRegex(html.lower(), r"accept|apply the suggestion|use this|one-click|auto")

    def test_the_decision_says_whether_the_suggestion_was_opened_first(self):
        r = run("decisions", routes=[{"match": "/recommendations\\?outcome=open", "body": {"items": [{"candidate_id": "cand_b"}]}}])      # the server still lists b, no longer a
        posts = {p["url"].split("/")[-2]: p["body"] for p in r["posts"]}
        self.assertEqual(posts["cand_a"], {"decision": "confirm", "suggestion_viewed": True})        # opened it
        self.assertEqual(posts["cand_b"], {"decision": "reject", "suggestion_viewed": False})        # one was there, never opened
        self.assertEqual(posts["cand_c"], {"decision": "confirm"})                                    # none: nothing claimed either way
        for name, ids in r["state"].items():
            self.assertFalse({"cand_a", "cand_b", "cand_c"} & set(ids) - ({"cand_b"} if name == "hints" else set()), name)     # a decided item leaves every set (the fake server keeps listing b)

    def test_closing_a_panel_does_not_un_see_it(self):
        r = run("close_keeps_viewed", routes=[{"match": "/review-queue/cand_1/recommendations", "body": {"items": [{"recommendation": "reject", "confidence": 0.7, "reasons": ["SECRET-REASON"]}]}}])
        self.assertEqual((r["state"]["open"], r["state"]["viewed"]), ([], ["cand_1"]))
        self.assertNotIn("SECRET-REASON", r["html"])                                                  # and a closed panel shows nothing again
        r = run("opened_then_closed_then_decided", routes=[{"match": "/review-queue/cand_a/recommendations", "body": {"items": []}}])
        self.assertEqual(r["posts"], [{"decision": "confirm", "suggestion_viewed": True}])

    def test_an_open_panel_stays_open_through_the_refresh_and_the_others_stay_closed(self):
        html = run("refresh")["html"]
        self.assertIn('data-id="cand_1" open', html)
        self.assertNotIn('data-id="cand_2" open', html)
        self.assertEqual(html.count("SECRET-REASON"), 1)                                              # only the open one carries text

    def test_which_items_have_a_suggestion_is_learned_in_one_request_and_the_screen_only_redraws_when_that_changes(self):
        r = run("hints", routes=[{"match": "/recommendations\\?outcome=open", "body": {"items": [{"candidate_id": "cand_1"}, {"candidate_id": "cand_2"}]}}])
        self.assertEqual((r["first"], r["second"]), (1, 0))
        self.assertEqual(sorted(r["hints"]), ["cand_1", "cand_2"])
        self.assertEqual(len(r["urls"]), 2)
        self.assertTrue(all("outcome=open" in u for u in r["urls"]))

    def test_a_failing_api_changes_nothing_on_screen_and_an_empty_list_clears_it(self):
        r = run("hints_fail", routes=[{"match": "/recommendations", "status": 500}])
        self.assertEqual(r["state"]["hints"], ["cand_old"])                                           # could not ask: what was known is kept, so an open panel does not vanish
        r = run("hints_fail", routes=[{"match": "/recommendations", "body": {"items": []}}])
        self.assertEqual(r["state"]["hints"], [])                                                     # asked, and there are none: that is the answer
        self.assertNotIn("suggest", r["html"].replace("suggestion", ""))
        r = run("load_fail", routes=[{"match": "/review-queue/cand_1/recommendations", "status": 500}])
        self.assertIn("Could not load the suggestion", r["html"])
        self.assertEqual(r["state"]["viewed"], ["cand_1"])                                            # they did open it, and that is what the decision reports

    def test_how_reliable_it_has_been_is_shown_with_how_few_cases_that_is_based_on(self):
        r = run("stats", stats={"few": {"decided": 5, "agreed": 4}, "many": {"decided": 40, "agreed": 31}, "none": {"decided": 0, "agreed": 0}, "absent": None})
        self.assertIn("4 of 5 decided cases (too few to say how reliable it is)", r["few"])
        self.assertIn("31 of 40 decided cases.", r["many"])
        for name in ("none", "absent"):
            self.assertNotIn("decided cases", r[name])                                                # nothing claimed with no evidence
            self.assertIn("You decide.", r[name])

    def test_the_confidence_is_shown_as_a_percentage_and_garbage_is_not_shown_as_one(self):
        r = run("confidence", values=[0.8, 1, 0, 0.504, "high", None])
        self.assertIn("80% sure", r["0.8"])
        self.assertIn("100% sure", r["1"])
        self.assertIn("0% sure", r["0"])
        self.assertIn("50% sure", r["0.504"])
        self.assertNotIn("sure", r["high"].replace("assistant", ""))
        self.assertNotIn("NaN", r["high"] + r["null"])

    def test_switching_domain_forgets_everything_about_the_other_domains_suggestions(self):
        r = run("switch_domain")
        self.assertEqual({k: v for k, v in r["state"].items()}, {"hints": [], "open": [], "viewed": [], "loaded": [], "errors": []})
        self.assertIsNone(r["stats"])


if __name__ == "__main__":
    unittest.main()
