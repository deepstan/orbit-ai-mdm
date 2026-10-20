# System diagram

`../mdm-system-diagram.pdf` has two pages:

1. **Today**: what is in the application: ONE matching engine (the kernel) for every way a record arrives, what it keeps from before, the legacy engines kept for rollback, and the store, dialect and governance layers.
2. **What remains**: the steps from one engine to the 100-million-record target (batched writes, fewer statements, Gate P on PostgreSQL, Gate S), and what is still prototype-only.

Colors say how far each box has been PROVEN: green = in the application and tested; blue = prototype in `experiments/` (tested there); amber = built but never run on a PostgreSQL server; grey dashed = not built.

To regenerate after a change (Graphviz): 

```
dot -Tpdf architecture-today.dot  -o p1.pdf
dot -Tpdf architecture-target.dot -o p2.pdf
pdfunite p1.pdf p2.pdf ../mdm-system-diagram.pdf
```

Keep the colors honest: when something moves from prototype to the application, or Gate P has been run on a real server, change its color in the `.dot` file in the same change.
