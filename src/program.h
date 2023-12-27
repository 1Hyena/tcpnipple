// SPDX-License-Identifier: MIT
#ifndef PROGRAM_H_16_01_2021
#define PROGRAM_H_16_01_2021

#include <string>
#include <sys/time.h>
#include <cstdint>

class PROGRAM {
    public:

    PROGRAM(
        const char *name,
        const char *version)
    : pname(name)
    , pver(version)
    , status(EXIT_FAILURE)
    , options(nullptr)
    , signals(nullptr)
    , sockets(nullptr) {}

    ~PROGRAM() {}

    static size_t get_log_size();

    static void print_log(
        const char *, const char *, ...
    ) __attribute__((format(printf, 2, 3)));

    void log(const char *, ...) __attribute__((format(printf, 2, 3)));

    void bug(const char * =__builtin_FILE(), int =__builtin_LINE());
    bool init(int argc, char **argv);
    void run();
    int deinit();
    int get_status() const;

    const char *get_name() const;
    const char *get_version() const;
    uint16_t get_supply_port() const;
    uint16_t get_demand_port() const;
    const char *get_supply_host() const;
    const char *get_demand_host() const;
    bool is_verbose() const;
    uint16_t get_connection_count() const;

    private:
    static bool print_text(FILE *fp, const char *text, size_t length);
    static void write_time(char *buffer, size_t length);

    std::string    pname;
    std::string    pver;
    int            status;
    class OPTIONS *options;
    class SIGNALS *signals;
    class SOCKETS *sockets;

    static size_t log_size;
    static bool   log_time;
};

#endif
