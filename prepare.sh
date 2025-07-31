#!/usr/bin/env bash

./.venv/bin/python3 merge_data.py
./.venv/bin/python3 aggregate_data.py
./.venv/bin/python3 interpolate_improved.py
./.venv/bin/python3 clean_null_values.py