// SPDX-License-Identifier: MIT
#include <iostream>
#include <stdarg.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
    static constexpr const char
        *ansi_G = "\x1B[1;32m",
        *ansi_R = "\x1B[1;31m",
        *ansi_x = "\x1B[0m";

    if (!options) return bug();

    if (options->exit_flag) {
        status = EXIT_SUCCESS;
        return;
    }

    sockets->set_logger(
        [](SOCKETS::SESSION session, const char *text) noexcept {
            std::string line;
            char time[20];

            write_time(time, sizeof(time));
            line.append(time).append(" :: ");

            if (!session) {
                line.append("Sockets: ");
            }
            else {
                char buffer[20];
                std::snprintf(buffer, 20, "#%06lx: ", session.id);
                line.append(buffer);
            }

            const char *esc = "\x1B[0;31m";

            switch (session.error) {
                case SOCKETS::BAD_TIMING:    esc = "\x1B[1;33m"; break;
                case SOCKETS::LIBRARY_ERROR: esc = "\x1B[1;31m"; break;
                case SOCKETS::NO_ERROR:      esc = "\x1B[0;32m"; break;
                default: break;
            }

            line.append(esc).append(text).append("\x1B[0m").append("\n");
            print_text(stderr, line.c_str(), line.size());
        }
    );

    bool terminated = false;
    size_t total_connections = get_connection_count();

    std::unordered_map<size_t, size_t> supply_map;
    std::unordered_map<size_t, size_t> demand_map;
    std::unordered_set<size_t> unmet_supply;
    std::unordered_set<size_t> unmet_demand;
    std::unordered_set<size_t> sessions;
    std::unordered_set<size_t> suppliers;
    std::unordered_set<size_t> demanders;

    bool connecting = false;

    for (size_t i=0; i<total_connections; ++i) {
        SOCKETS::SESSION supplier{
            sockets->connect(
                get_supply_host(), std::to_string(get_supply_port()).c_str()
            )
        };

        if (supplier.valid) {
            suppliers.insert(supplier.id);
            sessions.insert(supplier.id);
        }

        SOCKETS::SESSION demander{
            sockets->connect(
                get_demand_host(), std::to_string(get_demand_port()).c_str()
            )
        };

        if (demander.valid) {
            demanders.insert(demander.id);
            sessions.insert(demander.id);
        }

        connecting = !supplier.error && !demander.error;
    }

    if (!total_connections) {
        terminated = true;
        status = EXIT_SUCCESS;

        if (is_verbose()) {
            log_time = true;

            log(
                "Opening no links from %s:%d to %s:%d.",
                get_supply_host(), int(get_supply_port()),
                get_demand_host(), int(get_demand_port())
            );
        }
    }
    else if (!connecting) {
        terminated = true;
        status = EXIT_FAILURE;
    }
    else {
        status = EXIT_SUCCESS;
        log_time = true;

        log(
            "Opening %lu link%s from %s:%d to %s:%d.",
            total_connections, total_connections == 1 ? "" : "s",
            get_supply_host(), int(get_supply_port()),
            get_demand_host(), int(get_demand_port())
        );

        for (size_t sid : sessions) {
            log(
                "Session %s#%06lx%s provides %s from %s:%s.",
                suppliers.count(sid) ? ansi_G :
                demanders.count(sid) ? ansi_R : "", sid, ansi_x,
                suppliers.count(sid) ? "supply" :
                demanders.count(sid) ? "demand" : "nothing",
                sockets->get_host(sid), sockets->get_port(sid)
            );
        }
    }

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
            for (auto sid : sessions) {
                sockets->disconnect(sid);
            }

            continue;
        }

        SOCKETS::ERROR next_error = sockets->next_error();
        if (next_error != SOCKETS::NO_ERROR) {
            log(
                "Error while serving sockets (%s).",
                sockets->to_string(next_error)
            );
            status = EXIT_FAILURE;
            terminated = true;
        }

        SOCKETS::ALERT alert;

        while ((alert = sockets->next_alert()).valid) {
            const size_t sid = alert.session;
            size_t other = 0;

            if (alert.event == SOCKETS::DISCONNECTION) {
                if (suppliers.count(sid)) {
                    log(
                        "Supply provider %s#%06lx%s has been disconnected.",
                        ansi_G, sid, ansi_x
                    );

                    suppliers.erase(sid);
                }
                else if (demanders.count(sid)) {
                    log(
                        "Demand provider %s#%06lx%s has been disconnected.",
                        ansi_R, sid, ansi_x
                    );

                    demanders.erase(sid);
                }
                else {
                    log(
                        "Strange session #%06lx has been disconnected.", sid
                    );

                    // Should never happen.
                    terminated = true;
                }

                sessions.erase(sid);

                if (supply_map.count(sid)) {
                    other = supply_map[sid];
                    supply_map.erase(sid);
                }
                else if (demand_map.count(sid)) {
                    other = demand_map[sid];
                    demand_map.erase(sid);
                }
                else if (unmet_supply.count(sid)) {
                    unmet_supply.erase(sid);
                }
                else if (unmet_demand.count(sid)) {
                    unmet_demand.erase(sid);
                }

                if (other) {
                    if (supply_map.count(other)) {
                        supply_map[other] = 0;
                    }
                    else if (demand_map.count(other)) {
                        demand_map[other] = 0;
                    }

                    sockets->disconnect(other);
                }
                else if (sessions.empty()) {
                    terminated = true;
                }
            }
            else if (alert.event == SOCKETS::CONNECTION) {
                if (suppliers.count(sid)) {
                    log(
                        "Session %s#%06lx%s is now ready to provide supply.",
                        ansi_G, sid, ansi_x
                    );

                    if (unmet_demand.empty()) {
                        unmet_supply.insert(sid);
                        sockets->freeze(sid);
                    }
                    else {
                        other = *(unmet_demand.begin());
                        unmet_demand.erase(other);
                        demand_map[other] = sid;
                        supply_map[sid] = other;
                        sockets->unfreeze(other);

                        log(
                            "Sessions %s#%06lx%s and %s#%06lx%s are now fused.",
                            ansi_G, sid, ansi_x, ansi_R, other, ansi_x
                        );
                    }
                }
                else if (demanders.count(sid)) {
                    log(
                        "Session %s#%06lx%s is now ready to provide demand.",
                        ansi_R, sid, ansi_x
                    );

                    if (unmet_supply.empty()) {
                        unmet_demand.insert(sid);
                        sockets->freeze(sid);
                    }
                    else {
                        other = *(unmet_supply.begin());
                        unmet_supply.erase(other);
                        supply_map[other] = sid;
                        demand_map[sid] = other;
                        sockets->unfreeze(other);

                        log(
                            "Sessions %s#%06lx%s and %s#%06lx%s are now fused.",
                            ansi_R, sid, ansi_x, ansi_G, other, ansi_x
                        );
                    }
                }
                else {
                    log(
                        "Session #%06lx of %s:%s exists for no reason.",
                        sid, sockets->get_host(sid), sockets->get_port(sid)
                    );

                    // Should never happen.
                    terminated = true;
                }
            }
            else if (alert.event == SOCKETS::INCOMING) {
                size_t forward_to = 0;

                if (supply_map.count(sid)) {
                    forward_to = supply_map[sid];

                }
                else if (demand_map.count(sid)) {
                    forward_to = demand_map[sid];
                }

                if (forward_to) {
                    const size_t size = sockets->get_incoming_size(sid);
                    const char *data = sockets->peek(sid);

                    SOCKETS::ERROR error{
                        sockets->write(forward_to, data, size)
                    };

                    if (!error) {
                        sockets->read(sid);

                        if (is_verbose()) {
                            log(
                                "%lu byte%s from %s#%06lx%s %s sent to "
                                "%s#%06lx%s.",
                                size, size == 1 ? "" : "s",
                                supply_map.count(sid) ? ansi_G : ansi_R, sid,
                                ansi_x,
                                size == 1 ? "is" : "are",
                                supply_map.count(forward_to) ? ansi_G : ansi_R,
                                forward_to, ansi_x
                            );
                        }
                    }
                    else {
                        log(
                            "%lu byte%s from %s#%06lx%s %s not sent to "
                            "%s#%06lx%s (%s).",
                            size, size == 1 ? "" : "s",
                            supply_map.count(sid) ? ansi_G : ansi_R, sid,
                            ansi_x,
                            size == 1 ? "is" : "are",
                            supply_map.count(forward_to) ? ansi_G : ansi_R,
                            forward_to, ansi_x, sockets->to_string(error)
                        );

                        sockets->disconnect(forward_to);
                        sockets->disconnect(sid);
                    }
                }
            }
        }

        if (supply_map.empty() && demand_map.empty()) {
            if (demanders.empty() || suppliers.empty()) {
                terminated = true;
            }

            continue;
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

    sockets = new (std::nothrow) SOCKETS();
    if (!sockets) return false;

    return sockets->init();
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

void PROGRAM::write_time(char *buffer, size_t length) {
    struct timeval timeofday;
    gettimeofday(&timeofday, nullptr);

    time_t timestamp = (time_t) timeofday.tv_sec;
    struct tm *tm_ptr = gmtime(&timestamp);

    if (!strftime(buffer, length, "%Y-%m-%d %H:%M:%S", tm_ptr)) {
        buffer[0] = '\0';
    }
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
        write_time(timebuf, sizeof(timebuf));
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

uint16_t PROGRAM::get_connection_count() const {
    return options->connections;
}
