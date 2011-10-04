#include <check.h>
#include <stdlib.h>
#include "hash.h"

START_TEST
(test_filehash_simple_input)
{
    fail();
}
END_TEST

Suite *
check_filehash_suite (void)
{
    Suite *s = suite_create("check_filehash");
    TCase *tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_filehash_simple_input);

    suite_add_tcase(s, tc_core);

    return s;
}

int
main (void)
{
    int number_failed;

    Suite *s = check_filehash_suite();
    SRunner *sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

/* EOF */
