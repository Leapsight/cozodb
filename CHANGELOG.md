# CHANGELOG

# 0.2.10
* Upgraded Cozo dependency which now offers 3 new temporal functions
    1. expand_daily(h0, h1, tz, start, end) - Lines 3926-4007
        - Expands daily recurrence to concrete intervals                       
        - Signature: (i64, i64, String, i64, i64) -> [[i64, i64]]               
    2. expand_monthly(day_of_month, h0, h1, tz, start, end) - Lines 4009-4109
        - Expands monthly recurrence to concrete intervals
        - Day clamping for months with fewer days (e.g., Feb 28/29)
        - Signature: (i64, i64, i64, String, i64, i64) -> [[i64, i64]]      
    3. expand_yearly(month, day, h0, h1, tz, start, end) - Lines 4111-4211
        - Expands yearly recurrence to concrete intervals
        - Skips Feb 29 on non-leap years (doesn't clamp to Feb 28)
        - Signature: (i64, i64, i64, i64, String, i64, i64) -> [[i64, i64]]
    4. Key Features
        - All timestamps in milliseconds
        - Proper DST handling using chrono-tz
        - End-of-day support (h1=1440 uses next day's midnight)
        - Input validation for month, day, day_of_month ranges
        - Timezone validation with IANA timezone strings
        - Overlap filtering - only includes intervals that overlap with [start, end]

# 0.2.9
* Fix error return types (broken with new cozo update in previous commit)

# 0.2.8
* Upgraded to latest version of forked cozo containing new temporal functions
* Improved capture of Cozo errors

# 0.2.7
* Fix rust dep graph_builder compilation issue with latest rayon (pinned working versions)

# 0.2.5
* Added `telemetry_registry` event declarations
# 0.2.0
* Require OTP27 with the new `json` module
