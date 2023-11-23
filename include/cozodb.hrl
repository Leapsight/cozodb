-define(ERROR(Reason, Args, Cause), ?ERROR(Reason, Args, Cause, #{})).

-define(ERROR(Reason, Args, Cause, Meta),
    erlang:error(Reason, Args, ?ERROR_OPTS(Cause, Meta))
).

-define(ERROR_OPTS(Cause), ?ERROR_OPTS(Cause, #{})).

-define(ERROR_OPTS(Cause, Meta),
    [{error_info, #{module => ?MODULE, cause => Cause, meta => Meta}}]
).
