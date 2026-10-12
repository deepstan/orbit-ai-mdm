"""
The Postgres SQL, checked statically (no server needed): the placeholder and percent mistakes that otherwise only appear when it first runs on Postgres.

Run with:  python3 -m unittest tests.test_postgres_sql_lint -v
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pglint  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")


def lint_text(code: str):
    path = os.path.join(tempfile.mkdtemp(), "sample.py")
    with open(path, "w") as f:
        f.write(code)
    return pglint.lint(path)[1]


class TestThePostgresStore(unittest.TestCase):
    def test_no_statement_has_a_placeholder_or_percent_mistake(self):
        strings, problems = pglint.lint(os.path.join(ROOT, "app", "db_postgres.py"))
        self.assertGreater(len(strings), 100)                       # it really looked at the SQL (and not at an empty file)
        self.assertEqual(problems, [], "\n".join(f"line {l}: {why}" for l, why, _ in problems))


class TestTheLinterCanFail(unittest.TestCase):
    """A check that cannot fail proves nothing."""

    def test_a_lone_percent_in_a_statement_with_parameters(self):
        found = lint_text('def f(cur):\n    cur.execute("SELECT 1 FROM t WHERE d LIKE \'system:%\' AND id = %s", (1,))\n')
        self.assertTrue(any("single '%'" in why for _, why, _ in found), found)

    def test_a_doubled_percent_is_fine(self):
        self.assertEqual(lint_text('def f(cur):\n    cur.execute("SELECT 1 FROM t WHERE d LIKE \'system:%%\' AND id = %s", (1,))\n'), [])

    def test_a_sqlite_style_placeholder(self):
        found = lint_text('def f(cur):\n    cur.execute("SELECT 1 FROM t WHERE id = ?", (1,))\n')
        self.assertTrue(any("'?' placeholder" in why for _, why, _ in found), found)

    def test_too_few_or_too_many_parameters(self):
        for params in ("(1,)", "(1, 2, 3)"):
            found = lint_text(f'def f(cur):\n    cur.execute("SELECT 1 FROM t WHERE a = %s AND b = %s", {params})\n')
            self.assertTrue(any("placeholders but" in why for _, why, _ in found), (params, found))

    def test_interpolated_sql_is_not_miscounted(self):
        self.assertEqual(lint_text('def f(cur, part):\n    cur.execute(f"SELECT 1 FROM {part} WHERE a = %s", (1,))\n'), [])

    def test_a_docstring_is_not_sql(self):
        self.assertEqual(lint_text('def f():\n    """SELECT what? UPDATE the thing WHERE it is 100% needed."""\n'), [])


if __name__ == "__main__":
    unittest.main()
