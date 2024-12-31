"""Following https://www.getdaft.io/projects/docs/en/stable/10-min.html

Use large data from https://registry.opendata.aws/nyc-tlc-trip-records-pds/ for testing.
"""

# %%
import datetime as dt
from IPython.display import display

import daft
from daft import DataType, udf

# %%
# Use Ray
# Run `uv run ray start --head --port=6379` in the terminal.
daft.context.set_runner_ray("127.0.0.1:6379")

# %%
# Can I run daft?
df = daft.from_pydict(
    {
        "integers": [1, 2, 3, 4],
        "floats": [1.5, 2.5, 3.5, 4.5],
        "bools": [True, True, False, False],
        "strings": ["a", "b", "c", "d"],
        "bytes": [b"a", b"b", b"c", b"d"],
        "dates": [
            dt.date(1994, 1, 1),
            dt.date(1994, 1, 2),
            dt.date(1994, 1, 3),
            dt.date(1994, 1, 4),
        ],
        "lists": [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]],
        "nulls": [None, None, None, None],
    }
)

display(df)

# %%
df2 = daft.read_parquet("s3://daft-public-data/tutorials/10-min/sample-data-dog-owners-partitioned.pq/**")
df2.show(2)
df2.where(df2["country"] == "Canada").explain(show_all=True)