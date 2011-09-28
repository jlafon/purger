#include "state.h"

int
purger_state_check(int from_state, int to_state)
{
    return ( \
        purger_state_strict_change_valid(from_state, to_state) ? \
            PURGER_STATE_R_YES :                                 \
        purger_state_force_change_valid(from_state, to_state)  ? \
            PURGER_STATE_R_FORCE :                               \
        PURGER_STATE_R_NO                                        \
    );
}

int
purger_state_strict_change_valid(int from_state, int to_state)
{
    return ( \
        (from_state == PURGER_STATE_START &&     \
         to_state == PURGER_STATE_TREEWALK) ||   \
        (from_state == PURGER_STATE_TREEWALK &&  \
         to_state == PURGER_STATE_WARNUSERS) ||  \
        (from_state == PURGER_STATE_WARNUSERS && \
         to_state == PURGER_STATE_REAPER) ||     \
        (from_state == PURGER_STATE_REAPER &&    \
         to_state == PURGER_STATE_TREEWALK)      \
    );
}

int
purger_state_force_change_valid(int from_state, int to_state)
{
    return( \
        purger_state_strict_change_valid(from_state, to_state) || \
        (from_state == to_state) \
    );
}

/* EOF */
