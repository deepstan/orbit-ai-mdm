"""
A static check of the SQL written for Postgres (app/db_postgres.py), for the mistakes that only show up on a live server: a SQLite-style `?` placeholder, a single `%`
(a LIKE pattern) in a statement that is sent with parameters, which psycopg2 reads as a placeholder, and a literal SQL string with a literal tuple of parameters of a
different length. It cannot judge whether the SQL is valid Postgres: only a Postgres can.
"""
import ast, re, sys
KEYWORDS = re.compile(r"\b(SELECT|INSERT INTO|UPDATE|DELETE FROM|CREATE|ALTER|DROP|WHERE)\b")

def text_of(node):
    """The SQL text of a string expression, with {X} where an f-string interpolates; None if it is not simply a string."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "{X}" for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        a, b = text_of(node.left), text_of(node.right)
        return None if a is None or b is None else a + b
    return None

def lint(path):
    tree = ast.parse(open(path).read())
    problems = []
    strings = []
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body and isinstance(node.body[0], ast.Expr) \
                and isinstance(node.body[0].value, ast.Constant) and isinstance(node.body[0].value.value, str):
            docstrings.add(id(node.body[0].value))
    for node in ast.walk(tree):
        if id(node) in docstrings:
            continue
        if isinstance(node, (ast.Constant, ast.JoinedStr)):
            t = text_of(node)
            if t and KEYWORDS.search(t) and len(t) > 12:
                strings.append((node.lineno, t))
    seen = set()
    for line, t in strings:
        key = (line, t)
        if key in seen: continue
        seen.add(key)
        flat = t.replace("\n", " ")
        if "?" in re.sub(r"'[^']*'", "''", t):
            problems.append((line, "a '?' placeholder (SQLite style) in Postgres SQL", flat[:110]))
        bare = t.replace("%%", "\x00\x00")                       # a doubled percent is a literal percent: take the pair as a unit
        for m in re.finditer(r"%(?![s(])", bare):
            problems.append((line, f"a single '%' followed by {bare[m.end():m.end()+1]!r}: psycopg2 reads it as a placeholder when parameters are passed", flat[max(0, m.start() - 40):m.start() + 40]))
            break
    # call level: a fully literal SQL string with a literal tuple of parameters must have as many %s as parameters
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("execute",) and len(node.args) == 2:
            sql, params = text_of(node.args[0]), node.args[1]
            if sql and "{X}" not in sql and isinstance(params, ast.Tuple) and not any(isinstance(e, ast.Starred) for e in params.elts):
                n = len(re.findall(r"%s", sql)); 
                if n != len(params.elts):
                    problems.append((node.lineno, f"{n} placeholders but {len(params.elts)} parameters", sql.replace("\n", " ")[:110]))
    return strings, problems

if __name__ == "__main__":
    strings, problems = lint(sys.argv[1] if len(sys.argv) > 1 else "app/db_postgres.py")
    print(f"{sys.argv[1]}: {len(strings)} SQL strings checked, {len(problems)} problem(s)")
    for line, why, ctx in sorted(problems): print(f"  line {line}: {why}\n      ...{ctx}")
