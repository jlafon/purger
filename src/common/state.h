#ifndef STATE_H
#define STATE_H

#define PURGER_STATE_START     0
#define PURGER_STATE_TREEWALK  1
#define PURGER_STATE_WARNUSERS 2
#define PURGER_STATE_REAPER    4

#define PURGER_STATE_R_NO      0
#define PURGER_STATE_R_YES     1
#define PURGER_STATE_R_FORCE   8

int purger_state_check              (int from_state, int to_state);
int purger_state_strict_change_valid(int from_state, int to_state);
int purger_state_force_change_valid (int from_state, int to_state);

#endif /* STATE_H */
