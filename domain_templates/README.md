# Domain config templates

Not loaded automatically. Copy what you need into your config directory (`./domains`, or the
directory in `$MDM_DOMAINS_DIR`) and restart the app.

- `customer.json`, `product.json`, `vendor.json` — the built-in domains, exactly as shipped, with
  `"override": true` already set. Copy one, edit it, and it replaces the built-in. Copying one
  *unchanged* changes nothing. (Regenerate any time: `python3 domains_cli.py export customer`.)
- `person.json` — a new domain with SSN and date of birth, using the `ssn` and `dob` cleansing steps (`parse`, `options`), a date-aware DOB comparison, near-equal SSNs, and the `scalable_tight_plus` blocking set (a `transpose` phone key, whole-name keys, and conditional `unless_any` groups). The SSN is protected (`"protect": "hash"`), so using it needs `MDM_TOKEN_KEY` in the environment (see the README). The model for any domain that carries an SSN or DOB.
- `location.yaml` — a brand-new domain, fully commented. The model for adding your own.

Always run `python3 domains_cli.py validate` after editing, and `python3 domains_cli.py status`
to see whether the change needs a reindex.
