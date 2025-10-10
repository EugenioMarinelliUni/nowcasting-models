#!/usr/bin/env python3
import pandas as pd

def parse_date_with_formats(s, fmts):
    for f in fmts:
        try: return pd.to_datetime(s, format=f)
        except Exception: pass
    return pd.to_datetime(s)