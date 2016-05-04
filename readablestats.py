#!/usr/bin/env python
# -*- coding: utf-8 -*-

import profile
import pstats

stats = pstats.Stats('pyaxtTimeStats.profile')

# Clean up filenames for the report
# stats.strip_dirs()

# Sort the statistics by the cumulative time spent in the function
# stats.sort_stats('cumulative')

# Sort the statistics by the number of calls
stats.sort_stats('ncalls')

stats.print_stats()
