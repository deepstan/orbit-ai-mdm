# Domain config templates

Not loaded automatically. Copy what you need into your config directory (`./domains`, or the
directory in `$MDM_DOMAINS_DIR`) and restart the app.

- `customer.json`, `product.json`, `vendor.json` — the built-in domains, exactly as shipped, with
  `"override": true` already set. Copy one, edit it, and it replaces the built-in. Copying one
  *unchanged* changes nothing. (Regenerate any time: `python3 domains_cli.py export customer`.)
- `location.yaml` — a brand-new domain, fully commented. The model for adding your own.

Always run `python3 domains_cli.py validate` after editing, and `python3 domains_cli.py status`
to see whether the change needs a reindex.
