#include "log.h"

int main(int argc, char *argv[]){

  PURGER_LOG("main", "%s\n", "test");
  PURGER_ELOG("main", "%s\n", "test error message");

}
