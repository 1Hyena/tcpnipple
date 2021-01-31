// SPDX-License-Identifier: MIT
#ifndef OPTIONS_H_01_01_2021
#define OPTIONS_H_01_01_2021

#include <string>
#include <limits>
#include <getopt.h>

class OPTIONS {
    public:

    OPTIONS(
        const char *version,
        void      (*log_fun) (const char *, const char *, ...) =drop_log,
        const char *log_src ="Options"
    ) : verbose         (      0)
      , exit_flag       (      0)
      , supply_port     (      0)
      , demand_port     (      0)
      , connections     (      1)
      , name            (     "")
      , version         (version)
      , logfrom         (log_src)
      , log             (log_fun) {}

    ~OPTIONS() {}

    int verbose;
    int exit_flag;
    uint16_t supply_port;
    uint16_t demand_port;
    uint16_t connections;
    std::string name;
    std::string supply_host;
    std::string demand_host;

    static constexpr const char *usage{
        "Options:\n"
        "  -c  --connections   Number of connections to make (1).\n"
        "      --brief         Print brief information (default).\n"
        "  -h  --help          Display this usage information.\n"
        "      --verbose       Print verbose information.\n"
        "  -v  --version       Show version information.\n"
    };

    std::string print_usage() const {
        char line[256];

        std::snprintf(
            line, sizeof(line),
            "Usage: %s [options] "
            "supply-host supply-port demand-host demand-port\n",
            name.c_str()
        );

        std::string result(line);

        return result.append(usage);
    }

    bool init(int argc, char **argv) {
        int c;
        name = argv[0];

        while (1) {
            static struct option long_options[] = {
                // These options set a flag:
                {"brief",       no_argument,       &verbose,   0 },
                {"verbose",     no_argument,       &verbose,   1 },
                // These options may take an argument:
                {"help",        no_argument,       0,        'h' },
                {"version",     no_argument,       0,        'v' },
                {"connections", required_argument, 0,        'c' },
                {0,             0,                 0,          0 }
            };

            int option_index = 0;
            c = getopt_long(
                argc, argv, "hvc:", long_options, &option_index
            );

            if (c == -1) break; // End of command line parameters?

            switch (c) {
                case 0: {
                    // If this option sets a flag do nothing else.
                    if (long_options[option_index].flag != 0) break;

                    std::string buf="option ";
                    buf.append(long_options[option_index].name);

                    if (optarg) {
                        buf.append(" with arg ");
                        buf.append(optarg);
                    }

                    log(logfrom.c_str(), buf.c_str());
                    break;
                }
                case 'h': {
                    log(nullptr, "%s\n", print_usage().c_str());
                    exit_flag = 1;
                    break;
                }
                case 'v':
                    log(nullptr, "%s\n", version.c_str());
                    exit_flag = 1;
                    break;
                case 'c': {
                    int i = atoi(optarg);
                    if ((i == 0 && (optarg[0] != '0' || optarg[1] != '\0'))
                    ||  (i <  0 || i > std::numeric_limits<uint16_t>::max())) {
                        log(
                            logfrom.c_str(), "invalid connections: %s", optarg
                        );
                        return false;
                    }
                    else connections = uint16_t(i);
                    break;
                }
                case '?':
                    // getopt_long already printed an error message.
                    break;
                default: return false;
            }
        }

        if (exit_flag) return true;

        if (optind < argc) {
            supply_host.assign(argv[optind++]);
        }

        if (optind < argc) {
            const char *port_str = argv[optind++];
            int p = atoi(port_str);

            if (p <= 0 || p > std::numeric_limits<uint16_t>::max()) {
                log(
                    logfrom.c_str(), "invalid port number: %s", port_str
                );
                return false;
            }

            supply_port = uint16_t(p);
        }
        else {
            log(nullptr, "%s\n", print_usage().c_str());
            log(
                logfrom.c_str(), "%s", "missing argument: supply-port"
            );
            return false;
        }

        if (optind < argc) {
            demand_host.assign(argv[optind++]);
        }

        if (optind < argc) {
            const char *port_str = argv[optind++];
            int p = atoi(port_str);

            if (p <= 0 || p > std::numeric_limits<uint16_t>::max()) {
                log(
                    logfrom.c_str(), "invalid port number: %s", port_str
                );
                return false;
            }

            demand_port = uint16_t(p);
        }
        else {
            log(nullptr, "%s\n", print_usage().c_str());
            log(
                logfrom.c_str(), "%s", "missing argument: demand-port"
            );
            return false;
        }

        while (optind < argc) {
            log(
                logfrom.c_str(), "unidentified argument: %s", argv[optind++]
            );
        }

        return true;
    }

    private:
    static void drop_log(const char *, const char *, ...) {}

    std::string version;
    std::string logfrom;
    void (*log)(const char *, const char *p_fmt, ...);
};

#endif
