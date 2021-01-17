// SPDX-License-Identifier: MIT
#include "program.h"

int main(int argc, char **argv) {
    int exit_status = EXIT_FAILURE;
    PROGRAM program(argv[0], "1.01");

    if (program.init(argc, argv)) {
        program.run();
        exit_status = program.deinit();
    }

    if (exit_status == EXIT_FAILURE) {
        if (PROGRAM::get_log_size() == 0) {
            PROGRAM::print_log(
                nullptr, "%s: %s\n", argv[0], "process exits with errors"
            );
        }
        else {
            PROGRAM::print_log("", "%s", "Process exits with errors.");
        }
    }

    return exit_status;
}
