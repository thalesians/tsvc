# thalesians.tsvc - Time Series Version Control

A lightweight, efficient version control system specifically designed for time series data.

## Overview

`thalesians.tsvc` provides a specialized solution for versioning time series datasets with a delta-based approach. This Python library offers fine-grained tracking of row-level and column-level changes, complete with automatic audit trails and efficient bidirectional versioning capabilities.

## Key features

* **Delta-based architecture:** Records only the changes between versions rather than complete snapshots.
* **Row-level operations:** Precisely tracks insertions, updates, and deletions at the individual record level.
* **Column-level operations:** Precisely tracks addition, renaming, deletion, and reordering of columns.
* **Metadata operations:** Precisely tracks changes to the time series metadata in the form of key-value pairs.
* **Bidirectional versioning:** Each change stores its inverse for efficient undo/redo capabilities.
* **Flexible persistence:** Choice between format-agnostic in-memory storage or on-disk serialization.
* **Pythonic design:** Clean, intuitive API that follows Python conventions.

## Why `thalesians.tsvc`?

Time series data presents unique versioning challenges that general-purpose VCS tools aren't optimized to handle:

* High-frequency updates to specific portions of data;
* Need for row-level and column-level (rather than file-level) change tracking;
* Performance requirements for large datasets;
* Specialized audit needs for financial and scientific contexts.

`thalesians.tsvc` addresses these challenges with a purpose-built solution that integrates seamlessly with Python data analysis workflows.

## Installation

<pre>
pip install thalesians.tsvc
</pre>
