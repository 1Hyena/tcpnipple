// SPDX-License-Identifier: MIT
#include <iostream>
#include <stdarg.h>

#include "options.h"
#include "program.h"
#include "signals.h"
#include "sockets.h"

volatile sig_atomic_t
    SIGNALS::sig_alarm{0},
    SIGNALS::sig_pipe {0},
    SIGNALS::sig_int  {0},
    SIGNALS::sig_term {0},
    SIGNALS::sig_quit {0};

size_t PROGRAM::log_size = 0;
bool   PROGRAM::log_time = false;

void PROGRAM::run() {
    if (!options) return bug();

    if (options->exit_flag) {
        status = EXIT_SUCCESS;
        return;
    }

    bool terminated = false;

    int supply_descriptor{
        sockets->connect(
            get_supply_host(), std::to_string(get_supply_port()).c_str()
        )
    };

    int demand_descriptor{
        sockets->connect(
            get_demand_host(), std::to_string(get_demand_port()).c_str()
        )
    };

    if (supply_descriptor == SOCKETS::NO_DESCRIPTOR
    ||  demand_descriptor == SOCKETS::NO_DESCRIPTOR) {
        terminated = true;
        status = EXIT_FAILURE;
    }
    else {
        status = EXIT_SUCCESS;
        log_time = true;

        log(
            "Connecting %s:%d to %s:%d...",
            get_supply_host(), int(get_supply_port()),
            get_demand_host(), int(get_demand_port())
        );
    }

    std::vector<uint8_t> buffer;

    do {
        signals->block();
        while (int sig = signals->next()) {
            char *sig_name = strsignal(sig);

            switch (sig) {
                case SIGINT :
                case SIGTERM:
                case SIGQUIT: terminated = true; // fall through
                default     : {
                    // Since signals are blocked, we can call fprintf here.
                    fprintf(stderr, "%s", "\n");

                    log(
                        "Caught signal %d (%s).", sig,
                        sig_name ? sig_name : "unknown"
                    );

                    break;
                }
            }
        }
        signals->unblock();

        if (terminated) {
            sockets->disconnect(demand_descriptor);
            sockets->disconnect(supply_descriptor);

            continue;
        }

        if (!sockets->serve()) {
            log("%s", "Error while serving sockets.");
            status = EXIT_FAILURE;
            terminated = true;
        }

        int d = SOCKETS::NO_DESCRIPTOR;
        while ((d = sockets->next_disconnection()) != SOCKETS::NO_DESCRIPTOR) {
            log(
                "Disconnected %s:%s (descriptor %d).",
                sockets->get_host(d), sockets->get_port(d), d
            );

            int other = SOCKETS::NO_DESCRIPTOR;

                 if (d == supply_descriptor) other = demand_descriptor;
            else if (d == demand_descriptor) other = supply_descriptor;

            if (other != SOCKETS::NO_DESCRIPTOR) {
                sockets->disconnect(other);
                terminated = true;
            }
        }

        while ((d = sockets->next_incoming()) != SOCKETS::NO_DESCRIPTOR) {
            sockets->swap_incoming(d, buffer);

            int forward_to = SOCKETS::NO_DESCRIPTOR;

                 if (d == supply_descriptor) forward_to = demand_descriptor;
            else if (d == demand_descriptor) forward_to = supply_descriptor;

            if (forward_to == SOCKETS::NO_DESCRIPTOR) {
                log("Forbidden condition met (%s:%d).", __FILE__, __LINE__);
            }
            else {
                if (is_verbose()) {
                    log(
                        "%lu byte%s from %s:%s %s sent to %s:%s.",
                        buffer.size(), buffer.size() == 1 ? "" : "s",
                        sockets->get_host(d), sockets->get_port(d),
                        buffer.size() == 1 ? "is" : "are",
                        sockets->get_host(forward_to),
                        sockets->get_port(forward_to)
                    );
                }

                sockets->append_outgoing(forward_to, buffer);
            }

            buffer.clear();
        }
    }
    while (!terminated);

    return;
}

bool PROGRAM::init(int argc, char **argv) {
    signals = new (std::nothrow) SIGNALS(print_log);
    if (!signals) return false;

    if (!signals->init()) {
        return false;
    }

    options = new (std::nothrow) OPTIONS(get_version(), print_log);
    if (!options) return false;

    if (!options->init(argc, argv)) {
        return false;
    }

    sockets = new (std::nothrow) SOCKETS(print_log);
    if (!sockets) return false;

    if (!sockets->init()) {
        return false;
    }

    return true;
}

int PROGRAM::deinit() {
    if (sockets) {
        if (!sockets->deinit()) {
            status = EXIT_FAILURE;
            bug();
        }

        delete sockets;
        sockets = nullptr;
    }

    if (options) {
        delete options;
        options = nullptr;
    }

    if (signals) {
        delete signals;
        signals = nullptr;
    }

    return get_status();
}

int PROGRAM::get_status() const {
    return status;
}

size_t PROGRAM::get_log_size() {
    return PROGRAM::log_size;
}

bool PROGRAM::print_text(FILE *fp, const char *text, size_t len) {
    // Because fwrite may be interrupted by a signal, we block them.

    sigset_t sigset_all;
    sigset_t sigset_orig;

    if (sigfillset(&sigset_all) == -1) {
        return false;
    }
    else if (sigprocmask(SIG_SETMASK, &sigset_all, &sigset_orig) == -1) {
        return false;
    }

    fwrite(text , sizeof(char), len, fp);

    if (sigprocmask(SIG_SETMASK, &sigset_orig, nullptr) == -1) {
        return false;
    }

    return true;
}

void PROGRAM::print_log(const char *origin, const char *p_fmt, ...) {
    va_list ap;
    char *buf = nullptr;
    char *newbuf = nullptr;
    int buffered = 0;
    int	size = 1024;

    if (p_fmt == nullptr) return;
    buf = (char *) malloc (size * sizeof (char));

    while (1) {
        va_start(ap, p_fmt);
        buffered = vsnprintf(buf, size, p_fmt, ap);
        va_end (ap);

        if (buffered > -1 && buffered < size) break;
        if (buffered > -1) size = buffered + 1;
        else               size *= 2;

        if ((newbuf = (char *) realloc (buf, size)) == nullptr) {
            free (buf);
            return;
        } else {
            buf = newbuf;
        }
    }

    std::string logline;
    logline.reserve(size);

    if (PROGRAM::log_time) {
        char timebuf[20];
        struct timeval timeofday;
        gettimeofday(&timeofday, nullptr);

        time_t timestamp = (time_t) timeofday.tv_sec;
        struct tm *tm_ptr = gmtime(&timestamp);

        if (!strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm_ptr)) {
            timebuf[0] = '\0';
        }

        logline.append(timebuf);
        logline.append(" :: ");
    }

    if (origin && *origin) {
        logline.append(origin);
        logline.append(": ");
    }

    logline.append(buf);

    if (origin) logline.append("\n");

    PROGRAM::log_size += logline.size();
    print_text(stderr, logline.c_str(), logline.size());
    free(buf);
}

void PROGRAM::log(const char *p_fmt, ...) {
    va_list ap;
    char *buf = nullptr;
    char *newbuf = nullptr;
    int buffered = 0;
    int	size = 1024;

    if (p_fmt == nullptr) return;
    buf = (char *) malloc (size * sizeof (char));

    while (1) {
        va_start(ap, p_fmt);
        buffered = vsnprintf(buf, size, p_fmt, ap);
        va_end (ap);

        if (buffered > -1 && buffered < size) break;
        if (buffered > -1) size = buffered + 1;
        else               size *= 2;

        if ((newbuf = (char *) realloc (buf, size)) == nullptr) {
            free (buf);
            return;
        } else {
            buf = newbuf;
        }
    }

    print_log("", "%s", buf);
    free(buf);
}

void PROGRAM::bug(const char *file, int line) {
    log("Bug on line %d of %s.", line, file);
}

const char *PROGRAM::get_name() const {
    return pname.c_str();
}

const char *PROGRAM::get_version() const {
    return pver.c_str();
}

const char *PROGRAM::get_supply_host() const {
    return options->supply_host.c_str();
}

uint16_t PROGRAM::get_supply_port() const {
    return options->supply_port;
}

const char *PROGRAM::get_demand_host() const {
    return options->demand_host.c_str();
}

uint16_t PROGRAM::get_demand_port() const {
    return options->demand_port;
}

bool PROGRAM::is_verbose() const {
    return options->verbose;
}
