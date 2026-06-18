# Deprecated modules

Legacy MongoDB backend and setup scripts. Not imported by `create_client` or the Coffeemaker workers.

To run manually (optional):

```bash
pip install pymongo deprecation
python -c "from pybeansack.deprecated.mongosack import MongoDB; ..."
```

`mongosetup.js` — historical MongoDB index definitions for Atlas/local mongo.
