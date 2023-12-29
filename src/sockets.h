////////////////////////////////////////////////////////////////////////////////
// MIT License                                                                //
//                                                                            //
// Copyright (c) 2023 Erich Erstu                                             //
//                                                                            //
// Permission is hereby granted, free of charge, to any person obtaining a    //
// copy of this software and associated documentation files (the "Software"), //
// to deal in the Software without restriction, including without limitation  //
// the rights to use, copy, modify, merge, publish, distribute, sublicense,   //
// and/or sell copies of the Software, and to permit persons to whom the      //
// Software is furnished to do so, subject to the following conditions:       //
//                                                                            //
// The above copyright notice and this permission notice shall be included in //
// all copies or substantial portions of the Software.                        //
//                                                                            //
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR //
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,   //
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL    //
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER //
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING    //
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER        //
// DEALINGS IN THE SOFTWARE.                                                  //
////////////////////////////////////////////////////////////////////////////////

#ifndef SOCKETS_H_05_01_2023
#define SOCKETS_H_05_01_2023

#include <algorithm>
#include <limits>

#include <csignal>
#include <cstring>
#include <cstdarg>
#include <cerrno>
#include <cstdio>
#include <cstdint>

#include <sys/epoll.h>
#include <netdb.h>
#include <unistd.h>

class SOCKETS final {
    public:
    static constexpr const char *const VERSION = "1.02";

    enum class ERROR : uint8_t {
        NONE = 0,
        BAD_TIMING,    // Try again later.
        PENDING_ALERT, // Handle all the pending alerts and try again.
        OUT_OF_MEMORY, // Free some memory and then try again.
        RES_LIMIT_MET, // System lacks resources to complete a task.
        LIBRARY,       // There is a bug in this library.
        BAD_REQUEST,   // There is a bug in the caller's application.
        SYSTEM,        // Standard Library's system call failed with an error.
        UNKNOWN        // Standard Library call failed for an unknown reason.
    };

    static constexpr const ERROR
        NO_ERROR       = ERROR::NONE,
        BAD_TIMING     = ERROR::BAD_TIMING,
        PENDING_ALERT  = ERROR::PENDING_ALERT,
        OUT_OF_MEMORY  = ERROR::OUT_OF_MEMORY,
        RES_LIMIT_MET  = ERROR::RES_LIMIT_MET,
        LIBRARY_ERROR  = ERROR::LIBRARY,
        BAD_REQUEST    = ERROR::BAD_REQUEST,
        SYSTEM_ERROR   = ERROR::SYSTEM,
        UNKNOWN_ERROR  = ERROR::UNKNOWN;

    inline static const char *to_string(ERROR) noexcept;

    struct SESSION {
        size_t id;
        ERROR error;
        bool valid:1;
    };

    enum class EVENT : uint8_t {
        NONE = 0,
        // Do not change the order of the events above this line.
        READ,
        WRITE,
        ACCEPT,
        CONNECTION,
        DISCONNECTION,
        CLOSE,
        INCOMING,
        // Do not change the order of the events below this line.
        EPOLL,
        MAX_EVENTS
    };

    static constexpr const EVENT
        NO_EVENT      = EVENT::NONE,
        CONNECTION    = EVENT::CONNECTION,
        DISCONNECTION = EVENT::DISCONNECTION,
        INCOMING      = EVENT::INCOMING;

    struct ALERT {
        size_t session;
        EVENT event;
        bool valid:1;
    };

    struct RESULT{
        int        value;
        int         code;
        const char *text;
        const char *call;
        const char *file;
        int         line;
        ERROR      error;
    };

    SOCKETS() noexcept;
    ~SOCKETS();

    bool init() noexcept;
    bool deinit() noexcept;

    void set_logger(void (*callback)(SESSION, const char *) noexcept) noexcept;
    void set_memcap(size_t bytes) noexcept;
    ERROR set_intake(
        size_t session_id, size_t bytes,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    size_t get_memcap() const noexcept;
    size_t get_memtop() const noexcept;

    SESSION listen(
        const char *port, int family =AF_UNSPEC,
        const std::initializer_list<int> options ={SO_REUSEADDR}, int flags =0
    ) noexcept;
    SESSION connect(
        const char *host, const char *port, int family =AF_UNSPEC
    ) noexcept;
    [[nodiscard]] bool idle() const noexcept;

    ERROR next_error(int timeout =-1) noexcept;
    ERROR last_error() noexcept;
    ALERT next_alert() noexcept;

    [[nodiscard]] size_t get_incoming_size(size_t session_id) const noexcept;
    [[nodiscard]] size_t get_outgoing_size(size_t session_id) const noexcept;
    size_t read(size_t session_id, void *buf, size_t count) noexcept;
    const char *read(size_t session_id) noexcept;
    const char *peek(size_t session_id) noexcept;
    ERROR write(size_t session_id, const void *buf, size_t count) noexcept;
    ERROR write(size_t session_id, const char *text) noexcept;
    ERROR writef(
        size_t session_id, const char *fmt, ...
    ) noexcept __attribute__((format(printf, 3, 4)));

    [[nodiscard]] bool is_listener(size_t session_id) const noexcept;
    [[nodiscard]] bool is_frozen(size_t session_id) const noexcept;
    [[nodiscard]] SESSION get_listener(size_t session_id) const noexcept;
    [[nodiscard]] const char *get_host(size_t session_id) const noexcept;
    [[nodiscard]] const char *get_port(size_t session_id) const noexcept;
    [[nodiscard]] SESSION get_session(int descriptor) const noexcept;

    void freeze(size_t session_id) noexcept;
    void unfreeze(size_t session_id) noexcept;
    void disconnect(size_t session_id) noexcept;

    static constexpr const size_t BITS_PER_BYTE{
        std::numeric_limits<unsigned char>::digits
    };

    private:
    enum class BUFFER : uint8_t {
        NEXT_ERROR,
        // Do not change the order of items below this line.
        MAX_BUFFERS
    };

    struct MEMORY {
        size_t     size;
        void      *data;
        MEMORY    *next;
        MEMORY    *prev;
        bool  indexed:1;
        bool recycled:1;
    };

    struct KEY {
        uintptr_t value;
    };

    struct PIPE {
        enum class TYPE : uint8_t {
            NONE = 0,
            UINT8,
            UINT64,
            INT,
            PTR,
            JACK_PTR,
            MEMORY_PTR,
            KEY,
            EPOLL_EVENT
        };

        struct ENTRY {
            union {
                uint8_t     as_uint8;
                uint64_t    as_uint64;
                int         as_int;
                void       *as_ptr;
                KEY         as_key;
                epoll_event as_epoll_event;
            };
            TYPE type;
        };

        size_t capacity;
        size_t size;
        void *data;
        TYPE type;
        MEMORY *memory;
    };

    struct INDEX {
        enum class TYPE : uint8_t {
            NONE = 0,
            // Do not change the order of the types above this line.
            EVENT_DESCRIPTOR,
            DESCRIPTOR_JACK,
            SESSION_JACK,
            RESOURCE_MEMORY,
            // Do not change the order of the types below this line.
            MAX_TYPES
        };

        struct ENTRY {
            PIPE *key_pipe;
            PIPE *val_pipe;
            size_t index;
            ERROR error;
            bool valid:1;
        };

        size_t buckets;
        size_t entries;
        struct TABLE {
            PIPE key;
            PIPE value;
        } *table;
        TYPE type;
        bool multimap:1;
        bool autogrow:1;
    };

    struct JACK {
        size_t id;
        size_t intake;
        unsigned event_lookup[ static_cast<size_t>(EVENT::MAX_EVENTS) ];
        PIPE epoll_ev;
        PIPE children;
        PIPE incoming;
        PIPE outgoing;
        PIPE host;
        PIPE port;
        int descriptor;
        struct PARENT {
            int descriptor;
            int child_index;
        } parent;
        int ai_family;
        int ai_flags;
        struct addrinfo *blacklist;
        struct BITSET {
            bool frozen:1;
            bool connecting:1;
            bool may_shutdown:1;
            bool reconnect:1;
            bool listener:1;
        } bitset;
    };

    struct QUERY {
        enum class TYPE : uint8_t {
            DESCRIPTOR,
            SESSION,
            EVENT
        };

        union {
            int descriptor;
            size_t session;
            EVENT event;
        };
        TYPE type;
    };

    inline static constexpr KEY make_key(uintptr_t) noexcept;
    inline static constexpr KEY make_key(EVENT) noexcept;

    inline static constexpr struct JACK make_jack(
        int descriptor, int parent =-1
    ) noexcept;

    inline static constexpr struct ALERT make_alert(
        size_t session, EVENT type, bool valid =true
    ) noexcept;

    inline static constexpr struct SESSION make_session(ERROR) noexcept;
    inline static constexpr struct SESSION make_session(
        size_t id, ERROR error =ERROR::NONE, bool valid =true
    ) noexcept;

    inline static constexpr struct INDEX::ENTRY make_index_entry(
        PIPE &keys, PIPE &values, size_t index, ERROR, bool valid
    ) noexcept;

    inline static constexpr struct INDEX::ENTRY make_index_entry(
        PIPE &keys, PIPE &values, size_t index, ERROR
    ) noexcept;

    inline static constexpr PIPE make_pipe(
        const uint8_t *data, size_t size
    ) noexcept;

    inline static constexpr PIPE make_pipe(PIPE::TYPE) noexcept;

    inline static constexpr PIPE::ENTRY make_pipe_entry(PIPE::TYPE) noexcept;
    inline static constexpr PIPE::ENTRY make_pipe_entry(uint64_t  ) noexcept;
    inline static constexpr PIPE::ENTRY make_pipe_entry(int       ) noexcept;
    inline static constexpr PIPE::ENTRY make_pipe_entry(KEY       ) noexcept;
    inline static constexpr PIPE::ENTRY make_pipe_entry(JACK *    ) noexcept;
    inline static constexpr PIPE::ENTRY make_pipe_entry(MEMORY *  ) noexcept;

    inline static constexpr epoll_data_t make_epoll_data(int fd) noexcept;
    inline static constexpr epoll_event make_epoll_event(
        int descriptor, uint32_t events
    ) noexcept;
    inline static constexpr struct addrinfo make_addrinfo(
        int ai_family, int ai_flags
    ) noexcept;

    inline static constexpr QUERY make_descriptor_query(int) noexcept;
    inline static constexpr QUERY make_session_query(size_t) noexcept;
    inline static constexpr QUERY make_session_query(SESSION) noexcept;
    inline static constexpr QUERY make_event_query(EVENT) noexcept;

    inline static bool is_listed(
        const addrinfo &info, const addrinfo *list
    ) noexcept;

    inline static constexpr bool is_descriptor(int) noexcept;
    inline static constexpr EVENT next(EVENT) noexcept;
    inline static constexpr size_t size(PIPE::TYPE) noexcept;
    inline static constexpr size_t align(PIPE::TYPE) noexcept;
    inline static constexpr auto fmt_bytes(size_t) noexcept;
    inline static constexpr const char *LEAF(const char *path) noexcept;
    inline static constexpr const char *TAIL(const char *, char neck) noexcept;
    inline static int clz(unsigned int) noexcept;
    inline static int clz(unsigned long) noexcept;
    inline static int clz(unsigned long long) noexcept;
    inline static unsigned int       next_pow2(unsigned int) noexcept;
    inline static unsigned long      next_pow2(unsigned long) noexcept;
    inline static unsigned long long next_pow2(unsigned long long) noexcept;

    ERROR handle_close (JACK &) noexcept;
    ERROR handle_epoll (JACK &, int timeout) noexcept;
    ERROR handle_read  (JACK &) noexcept;
    ERROR handle_write (JACK &) noexcept;
    ERROR handle_accept(JACK &) noexcept;

    SESSION connect(
        const char *host, const char *port, int family, int flags,
        JACK *predecessor =nullptr,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;

    SESSION listen(
        const char *host, const char *port, int family, int flags,
        const std::initializer_list<int> options, JACK *predecessor =nullptr,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;

    void terminate(
        int descriptor,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;

    size_t next_connection() noexcept;
    size_t next_disconnection() noexcept;
    size_t next_incoming() noexcept;

    SESSION create_epoll() noexcept;
    ERROR operate_epoll(int operation, epoll_event event) noexcept;
    ERROR bind_to_epoll(
        int descriptor,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    ERROR modify_epoll(
        int descriptor, uint32_t events,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    ERROR block_signals(sigset_t &sigset_orig) noexcept;
    ERROR unblock_signals(sigset_t &sigset_orig) noexcept;

    SESSION open_and_capture(
        const char *host, const char *port, int family, int flags,
        const std::initializer_list<int> options ={}, JACK *predecessor=nullptr,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;

    void close_and_release(
        JACK&,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    void close_descriptor(int) noexcept;

    [[nodiscard]] SESSION capture(const JACK &copy) noexcept;
    void release(JACK *) noexcept;

    JACK *find_jack(const QUERY &) const noexcept;
    JACK *find_epoll_jack() const noexcept;
    JACK &get_jack(const QUERY &) const noexcept;
    JACK &get_epoll_jack() const noexcept;
    const PIPE *find_descriptors(EVENT) const noexcept;

    void set_event(JACK &, EVENT, bool val =true) noexcept;
    void rem_event(JACK &, EVENT) noexcept;
    [[nodiscard]] bool has_event(const JACK &, EVENT) const noexcept;
    void rem_child(JACK &, JACK &child) const noexcept;
    [[nodiscard]] bool is_listener(const JACK &) const noexcept;

    size_t count(INDEX::TYPE, KEY key) const noexcept;
    INDEX::ENTRY find(
        INDEX::TYPE, KEY key, PIPE::ENTRY value ={},
        size_t start_i =std::numeric_limits<size_t>::max(),
        size_t iterations =std::numeric_limits<size_t>::max()
    ) const noexcept;
    size_t erase(
        INDEX::TYPE, KEY key, PIPE::ENTRY value ={},
        size_t start_i =std::numeric_limits<size_t>::max(),
        size_t iterations =std::numeric_limits<size_t>::max()
    ) noexcept;
    [[nodiscard]] ERROR reserve(INDEX::TYPE, KEY key, size_t capacity) noexcept;
    [[nodiscard]] INDEX::ENTRY insert(
        INDEX::TYPE, KEY key, PIPE::ENTRY value
    ) noexcept;
    [[nodiscard]] ERROR reindex() noexcept;
    void erase(PIPE &pipe, size_t index) const noexcept;
    void destroy(PIPE &pipe) noexcept;
    void set_value(INDEX::ENTRY, PIPE::ENTRY) noexcept;
    PIPE::ENTRY get_value(INDEX::ENTRY) const noexcept;
    PIPE::ENTRY get_entry(const PIPE &pipe, size_t index) const noexcept;
    PIPE::ENTRY get_last(const PIPE &pipe) const noexcept;
    PIPE::ENTRY pop_back(PIPE &pipe) const noexcept;
    [[nodiscard]] ERROR reserve(PIPE&, size_t capacity) noexcept;
    [[nodiscard]] ERROR insert(PIPE&, PIPE::ENTRY) noexcept;
    [[nodiscard]] ERROR insert(PIPE&, size_t index, PIPE::ENTRY) noexcept;
    [[nodiscard]] ERROR copy(const PIPE &src, PIPE &dst) noexcept;
    [[nodiscard]] ERROR append(const PIPE &src, PIPE &dst) noexcept;
    void replace(PIPE&, size_t index, PIPE::ENTRY) const noexcept;
    bool swap_sessions(JACK *, JACK *) noexcept;
    ERROR swap(PIPE &, PIPE &) noexcept;
    PIPE &get_buffer(BUFFER) noexcept;
    INDEX &get_index(INDEX::TYPE) noexcept;

    KEY       to_key   (PIPE::ENTRY) const noexcept;
    JACK     *to_jack  (PIPE::ENTRY) const noexcept;
    MEMORY   *to_memory(PIPE::ENTRY) const noexcept;
    int       to_int   (PIPE::ENTRY) const noexcept;
    uint64_t  to_uint64(PIPE::ENTRY) const noexcept;

    int         *to_int        (const PIPE &) const noexcept;
    char        *to_char       (const PIPE &) const noexcept;
    uint8_t     *to_uint8      (const PIPE &) const noexcept;
    uint64_t    *to_uint64     (const PIPE &) const noexcept;
    KEY         *to_key        (const PIPE &) const noexcept;
    void       **to_ptr        (const PIPE &) const noexcept;
    epoll_event *to_epoll_event(const PIPE &) const noexcept;

    void *to_ptr(PIPE::ENTRY &) const noexcept;
    void *to_ptr(PIPE &, size_t index) const noexcept;
    const void *to_ptr(const PIPE &, size_t index) const noexcept;

    void enlist(MEMORY &, MEMORY *&list) noexcept;
    void unlist(MEMORY &, MEMORY *&list) noexcept;
    const MEMORY *find_memory(const void *) const noexcept;
    MEMORY *find_memory(const void *) noexcept;
    const MEMORY &get_memory(const void *) const noexcept;
    MEMORY &get_memory(const void *) noexcept;
    [[nodiscard]] INDEX::TABLE *allocate_tables(size_t count) noexcept;
    void destroy_and_delete(INDEX::TABLE *tables, size_t count) noexcept;
    [[nodiscard]] MEMORY *allocate_and_index(
        size_t byte_count, size_t alignment, const void *copy =nullptr
    ) noexcept;
    [[nodiscard]] MEMORY *allocate(const size_t bytes, size_t align) noexcept;
    void deallocate(MEMORY &) noexcept;
    void recycle(MEMORY &) noexcept;
    [[nodiscard]] JACK *new_jack(const JACK *copy =nullptr) noexcept;

    ERROR report(
        SESSION, const char *fmt, ...
    ) const noexcept __attribute__((format(printf, 3, 4)));
    template<class... Args>
    inline void log(const char *fmt, Args&&... args) const noexcept {
        report(make_session(ERROR::NONE), fmt, std::forward<Args>(args)...);
    }
    ERROR report(
        ERROR,
        int line =__builtin_LINE(), const char *file =LEAF(__builtin_FILE()),
        char const *function = __builtin_FUNCTION()
    ) const noexcept;
    ERROR report(
        ERROR, int code, char const *function, const char *message,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;
    ERROR report_bug(
        const char *comment =nullptr,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;
    ERROR report_bad_request(
        const char *comment =nullptr,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;
    ERROR report_memory_exhaustion(
        const char *comment =nullptr,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;
    const RESULT &report(const RESULT &) noexcept;
    const RESULT &report(size_t session_id, const RESULT &) noexcept;
    const RESULT &report(const JACK *, const RESULT &) noexcept;
    bool fuse(
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;
    [[noreturn]] void die(
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;

    inline static constexpr RESULT make_result(
        int value, int, ERROR,
        const char *comment, const char *function, const char *file, int line
    ) noexcept;

    RESULT call_sigfillset(
        sigset_t *set,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_sigemptyset(
        sigset_t *set,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_epoll_create1(
        int flags,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_epoll_pwait(
        int epfd, struct epoll_event *events, int maxevents, int timeout,
        const sigset_t *sigmask,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_getsockopt(
        int sockfd, int level, int optname, void *optval, socklen_t *optlen,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_accept4(
        int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_getnameinfo(
        const struct sockaddr *addr, socklen_t addrlen, char *host,
        socklen_t hostlen, char *serv, socklen_t servlen, int flags,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_shutdown(
        int sockfd, int how,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_listen(
        int sockfd, int backlog,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_getsockname(
        int sockfd, struct sockaddr *addr, socklen_t *addrlen,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_epoll_ctl(
        int epfd, int op, int fd, struct epoll_event *event,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_pthread_sigmask(
        int how, const sigset_t *set, sigset_t *oldset,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) const noexcept;
    RESULT call_getaddrinfo(
        const char *node, const char *service, const struct addrinfo *hints,
        struct addrinfo **res,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_socket(
        int domain, int type, int protocol,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_setsockopt(
        int sockfd, int level, int optname, const void *optval,
        socklen_t optlen,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_bind(
        int sockfd, const struct sockaddr *addr, socklen_t addrlen,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_connect(
        int sockfd, const struct sockaddr *addr, socklen_t addrlen,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;
    RESULT call_close(
        int fd,
        const char *file =LEAF(__builtin_FILE()), int line =__builtin_LINE()
    ) noexcept;

    void clear() noexcept;
    ERROR err(ERROR) noexcept;

    void (*log_callback)(SESSION, const char *text) noexcept;
    INDEX indices[static_cast<size_t>(INDEX::TYPE::MAX_TYPES)];
    PIPE  buffers[static_cast<size_t>(BUFFER::MAX_BUFFERS)];

    struct MEMPOOL {
        MEMORY *free[sizeof(size_t) * BITS_PER_BYTE];
        MEMORY *list;
        size_t usage;
        size_t top;
        size_t cap;
        bool   oom:1;
    } mempool;

    inline static constexpr struct MEMPOOL make_mempool() noexcept;

    EVENT handled;
    ERROR errored;

    size_t last_jack_id;
    size_t jack_count;

    struct BITSET {
        bool alerted:1;
        bool timeout:1;
        bool reindex:1;
    } bitset;

    sigset_t sigset_all;
    sigset_t sigset_none;

    public:
    mutable uint8_t fuses[768];
};

bool operator!(SOCKETS::ERROR error) noexcept {
    return error == static_cast<SOCKETS::ERROR>(0);
}

bool operator!(SOCKETS::RESULT result) noexcept {
    return result.error != SOCKETS::ERROR::NONE || result.code != 0;
}

bool operator!(SOCKETS::SESSION session) noexcept {
    return session.valid == false;
}

SOCKETS::SOCKETS() noexcept :
    log_callback(nullptr), indices{}, buffers{}, mempool{make_mempool()},
    handled{}, errored{}, last_jack_id{}, jack_count{}, bitset{}, sigset_all{},
    sigset_none{}, fuses{} {
}

SOCKETS::~SOCKETS() {
    if (mempool.usage > sizeof(SOCKETS)) {
        log(
            "memory usage remains at %lu byte%s (leak?)",
            mempool.usage, mempool.usage == 1 ? "" : "s"
        );
    }

    for (INDEX &index : indices) {
        if (index.type == INDEX::TYPE::NONE) {
            continue;
        }

        log(
            "%s\n", "destroying instance without having it deinitialized first"
        );

        break;
    }
}

void SOCKETS::clear() noexcept {
    errored = ERROR::NONE;
    handled = EVENT::NONE;

    for (PIPE &buffer : buffers) {
        destroy(buffer);
    }

    for (INDEX &index : indices) {
        if (index.table) {
            destroy_and_delete(index.table, index.buckets);
            index.table = nullptr;
        }

        index.buckets = 0;
        index.entries = 0;
        index.type = INDEX::TYPE::NONE;
    }

    if (mempool.list) {
        report_bug(
            // We should have already explicitly deallocated all memory.
        );

        while (mempool.list) {
            deallocate(*mempool.list);
        }
    }

    for (MEMORY *&free : mempool.free) {
        while (free) {
            deallocate(*free);
        }
    }

    mempool.usage = sizeof(SOCKETS);
    mempool.top = mempool.usage;

    last_jack_id = 0;
    jack_count = 0;
    bitset = {};

    std::fill(fuses, fuses+sizeof(fuses), 0);
}

bool SOCKETS::init() noexcept {
    for (INDEX &index : indices) {
        if (index.type != INDEX::TYPE::NONE) {
            log("%s: already initialized", __FUNCTION__);

            return false;
        }
    }

    if (!report(call_sigfillset(&sigset_all))
    ||  !report(call_sigemptyset(&sigset_none))) {
        return false;
    }

    clear();

    for (INDEX &index : indices) {
        index.type = static_cast<INDEX::TYPE>(&index - &indices[0]);

        switch (index.type) {
            default: {
                index.buckets = 1;
                index.multimap = false;
                index.autogrow = true;
                break;
            }
            case INDEX::TYPE::EVENT_DESCRIPTOR: {
                index.buckets = static_cast<size_t>(EVENT::MAX_EVENTS);
                index.multimap = true;
                index.autogrow = false;
                break;
            }
        }

        switch (index.type) {
            case INDEX::TYPE::NONE: continue;
            case INDEX::TYPE::EVENT_DESCRIPTOR:
            case INDEX::TYPE::DESCRIPTOR_JACK:
            case INDEX::TYPE::SESSION_JACK:
            case INDEX::TYPE::RESOURCE_MEMORY: {
                index.table = allocate_tables(index.buckets);
                break;
            }
            default: die();
        }

        if (index.table == nullptr) {
            report_memory_exhaustion();
            clear();
            return false;
        }

        for (size_t j=0; j<index.buckets; ++j) {
            INDEX::TABLE &table = index.table[j];
            PIPE &key_pipe = table.key;
            PIPE &val_pipe = table.value;

            key_pipe.type = PIPE::TYPE::KEY;

            switch (index.type) {
                case INDEX::TYPE::SESSION_JACK:
                case INDEX::TYPE::DESCRIPTOR_JACK: {
                    val_pipe.type = PIPE::TYPE::JACK_PTR;
                    break;
                }
                case INDEX::TYPE::RESOURCE_MEMORY: {
                    val_pipe.type = PIPE::TYPE::MEMORY_PTR;
                    break;
                }
                case INDEX::TYPE::EVENT_DESCRIPTOR: {
                    val_pipe.type = PIPE::TYPE::INT;
                    break;
                }
                default: die();
            }

            if (val_pipe.type == PIPE::TYPE::NONE) {
                clear();
                report_bug();
                return false;
            }
        }
    }

    for (PIPE &pipe : buffers) {
        switch (static_cast<BUFFER>(&pipe - &buffers[0])) {
            case BUFFER::NEXT_ERROR: {
                pipe.type = PIPE::TYPE::INT;
                break;
            }
            case BUFFER::MAX_BUFFERS: die();
        }
    }

    SESSION epoll_session{ create_epoll() };

    if (!epoll_session.valid) {
        log(
            "%s: %s, %s (%s:%d)", __FUNCTION__,
            "epoll jack could not be created", to_string(epoll_session.error),
            LEAF(__FILE__), __LINE__
        );

        return false;
    }

    return true;
}

bool SOCKETS::deinit() noexcept {
    if (!find_epoll_jack()) {
        log("%s: already deinitialized", __FUNCTION__);

        return false;
    }

    bool success = true;

    INDEX &descriptor_jack = get_index(INDEX::TYPE::DESCRIPTOR_JACK);

    for (size_t bucket=0; bucket<descriptor_jack.buckets; ++bucket) {
        while (descriptor_jack.table[bucket].key.size) {
            JACK *const jack{
                to_jack(
                    get_last(descriptor_jack.table[bucket].value)
                )
            };

            close_and_release(*jack);
        }
    }

    clear();

    return success;
}

void SOCKETS::set_logger(
    void (*callback)(SOCKETS::SESSION, const char *) noexcept
) noexcept {
    log_callback = callback;
}

void SOCKETS::set_memcap(size_t bytes) noexcept {
    mempool.cap = bytes;
}

size_t SOCKETS::get_memcap() const noexcept {
    return mempool.cap;
}

size_t SOCKETS::get_memtop() const noexcept {
    return mempool.top;
}

SOCKETS::ERROR SOCKETS::set_intake(
    size_t sid, size_t bytes, const char *file, int line
) noexcept {
    JACK *jack = find_jack(make_session_query(sid));

    if (jack) {
        jack->intake = bytes;

        return ERROR::NONE;
    }

    return fuse() ? report(
        make_session(sid, ERROR::BAD_REQUEST),
        "session not found (%s:%d)", file, line
    ) : ERROR::BAD_REQUEST;
}

SOCKETS::SESSION SOCKETS::listen(
    const char *port, int family, const std::initializer_list<int> options,
    int flags
) noexcept {
    return listen(nullptr, port, family, AI_PASSIVE|flags, options);
}

struct SOCKETS::ALERT SOCKETS::next_alert() noexcept {
    size_t session;

    bitset.alerted = false;

    while (( session = next_connection() ) != 0) {
        return make_alert(session, EVENT::CONNECTION);
    }

    while (( session = next_incoming() ) != 0) {
        return make_alert(session, EVENT::INCOMING);
    }

    while (( session = next_disconnection() ) != 0) {
        return make_alert(session, EVENT::DISCONNECTION);
    }

    return make_alert(0, EVENT::NONE, false);
}

size_t SOCKETS::next_connection() noexcept {
    JACK *const jack = find_jack(make_event_query(EVENT::CONNECTION));

    if (jack) {
        rem_event(*jack, EVENT::CONNECTION);

        return jack->id;
    }

    return 0;
}

size_t SOCKETS::next_disconnection() noexcept {
    if (find_jack(make_event_query(EVENT::CONNECTION))) {
        // We postpone reporting any disconnections until the application
        // has acknowledged all the new incoming connections. This prevents
        // us from reporting a disconnection event before its respective
        // connection event is reported.

        return 0;
    }

    JACK *const jack = find_jack(make_event_query(EVENT::DISCONNECTION));

    if (jack) {
        set_event(*jack, EVENT::CLOSE);
        rem_event(*jack, EVENT::DISCONNECTION);

        return jack->id;
    }

    return 0;
}

size_t SOCKETS::next_incoming() noexcept {
    JACK *const jack = find_jack(make_event_query(EVENT::INCOMING));

    if (jack) {
        rem_event(*jack, EVENT::INCOMING);

        return jack->id;
    }

    return 0;
}

bool SOCKETS::is_listener(const JACK &jack) const noexcept {
    return jack.bitset.listener;
}

bool SOCKETS::is_listener(size_t sid) const noexcept {
    const JACK *const jack = find_jack(make_session_query(sid));
    return jack ? is_listener(*jack) : false;
}

SOCKETS::SESSION SOCKETS::get_listener(size_t sid) const noexcept {
    const JACK *const jack = find_jack(make_session_query(sid));

    if (!jack) {
        return make_session(ERROR::NONE);
    }

    const JACK *const parent{
        find_jack(make_descriptor_query(jack->parent.descriptor))
    };

    return parent ? make_session(parent->id) : make_session(ERROR::NONE);
}

const char *SOCKETS::get_host(size_t session_id) const noexcept {
    const JACK *const jack = find_jack(make_session_query(session_id));
    return jack && jack->host.size ? to_char(jack->host) : "";
}

const char *SOCKETS::get_port(size_t session_id) const noexcept {
    const JACK *const jack = find_jack(make_session_query(session_id));
    return jack && jack->port.size ? to_char(jack->port) : "";
}

SOCKETS::SESSION SOCKETS::get_session(int descriptor) const noexcept {
    const JACK *const jack = find_jack(make_descriptor_query(descriptor));
    return jack ? make_session(jack->id) : make_session(0, ERROR::NONE, false);
}

void SOCKETS::freeze(size_t sid) noexcept {
    JACK *const jack = find_jack(make_session_query(sid));

    if (jack) {
        jack->bitset.frozen = true;
    }
}

void SOCKETS::unfreeze(size_t sid) noexcept {
    JACK *const jack = find_jack(make_session_query(sid));

    if (jack) {
        if (!has_event(*jack, EVENT::DISCONNECTION)
        &&  !has_event(*jack, EVENT::CLOSE)) {
            jack->bitset.frozen = false;
        }
    }
}

bool SOCKETS::is_frozen(size_t sid) const noexcept {
    const JACK *const jack = find_jack(make_session_query(sid));

    return jack ? jack->bitset.frozen : false;
}

bool SOCKETS::idle() const noexcept {
    return bitset.timeout;
}

SOCKETS::SESSION SOCKETS::connect(
    const char *host, const char *port, int family
) noexcept {
    return connect(host, port, family, 0);
}

void SOCKETS::disconnect(size_t session_id) noexcept {
    JACK *jack = find_jack(make_session_query(session_id));

    if (jack) {
        terminate(jack->descriptor);
    }
}

SOCKETS::ERROR SOCKETS::err(ERROR e) noexcept {
    return (errored = e);
}

SOCKETS::ERROR SOCKETS::last_error() noexcept {
    return errored;
}

constexpr auto SOCKETS::fmt_bytes(size_t b) noexcept {
    constexpr const size_t one{1};
    struct format_type{
        double value;
        const char *unit;
    };

    return (
        (sizeof(b) * BITS_PER_BYTE > 40) && b > (one << 40) ? (
            format_type{
                double((long double)(b) / (long double)(one << 40)), "TiB"
            }
        ) :
        (sizeof(b) * BITS_PER_BYTE > 30) && b > (one << 30) ? (
            format_type{
                double((long double)(b) / (long double)(one << 30)), "GiB"
            }
        ) :
        (sizeof(b) * BITS_PER_BYTE > 20) && b > (one << 20) ? (
            format_type{ double(b) / double(one << 20), "MiB" }
        ) : format_type{ double(b) / double(one << 10), "KiB" }
    );
}

SOCKETS::ERROR SOCKETS::next_error(int timeout) noexcept {
    if (mempool.usage > mempool.top) {
        mempool.top = mempool.usage;

        log(
            "top memory usage is %.3f %s",
            fmt_bytes(mempool.top).value, fmt_bytes(mempool.top).unit
        );
    }

    if (mempool.oom) {
        mempool.oom = false;
        return err(ERROR::OUT_OF_MEMORY);
    }

    if (bitset.reindex) {
        ERROR error{ reindex() };

        if (error != ERROR::NONE) {
            return err(error);
        }
    }

    if (find_jack(make_event_query(EVENT::CONNECTION))) {
        // We postpone handling any descriptor events until the application has
        // acknowledged all the new incoming connections.

        if (!bitset.alerted) {
            bitset.alerted = true;
            return err(ERROR::NONE);
        }

        return err(ERROR::PENDING_ALERT);
    }

    PIPE &descriptor_buffer = get_buffer(BUFFER::NEXT_ERROR);

    if (handled == EVENT::NONE) {
        bitset.timeout = false;
        handled = next(handled);
    }

    for (; handled != EVENT::NONE; handled = next(handled)) {
        EVENT event = handled;

        switch (event) {
            case EVENT::INCOMING:
            case EVENT::CONNECTION:
            case EVENT::DISCONNECTION: {
                // This event has to be handled by the user. We ignore it here.

                continue;
            }
            default: break;
        }

        const PIPE *const event_subscribers = find_descriptors(event);

        if (!event_subscribers) continue;

        ERROR error = copy(*event_subscribers, descriptor_buffer);

        if (error != ERROR::NONE) {
            return err(error);
        }

        for (size_t j=0, sz=descriptor_buffer.size; j<sz; ++j) {
            int d = to_int(get_entry(descriptor_buffer, j));
            JACK *const jack = find_jack(make_descriptor_query(d));

            if (jack == nullptr) continue;

            ERROR error = ERROR::NONE;

            switch (event) {
                case EVENT::EPOLL: {
                    error = handle_epoll(*jack, timeout);
                    break;
                }
                case EVENT::CLOSE: {
                    if (has_event(*jack, EVENT::READ)
                    && !jack->bitset.frozen) {
                        // Unless this descriptor is frozen, we postpone
                        // normal closing until there is nothing left to
                        // read from this descriptor.

                        continue;
                    }

                    error = handle_close(*jack);
                    break;
                }
                case EVENT::ACCEPT: {
                    if (find_jack(make_event_query(EVENT::DISCONNECTION))
                    || jack->bitset.frozen) {
                        // We postpone the acceptance of new connections
                        // until all the recent disconnections have been
                        // acknowledged and the descriptor is not frozen.

                        continue;
                    }

                    error = handle_accept(*jack);
                    break;
                }
                case EVENT::WRITE: {
                    if (jack->bitset.frozen) {
                        continue;
                    }

                    error = handle_write(*jack);
                    break;
                }
                case EVENT::READ: {
                    if (jack->bitset.frozen) {
                        continue;
                    }

                    error = handle_read(*jack);
                    break;
                }
                default: {
                    error = ERROR::LIBRARY;

                    if (fuse()) {
                        report(
                            make_session(error),
                            "Event %lu of descriptor %d was not handled.",
                            static_cast<size_t>(event), d
                        );
                    }

                    break;
                }
            }

            if (error == ERROR::NONE) {
                continue;
            }

            return err(error);
        }
    }

    return err(ERROR::NONE);
}

size_t SOCKETS::get_incoming_size(size_t sid) const noexcept {
    const JACK *const jack = find_jack(make_session_query(sid));

    if (jack) {
        return jack->incoming.size;
    }

    if (fuse()) report_bad_request();

    return 0;
}

size_t SOCKETS::get_outgoing_size(size_t sid) const noexcept {
    const JACK *const jack = find_jack(make_session_query(sid));

    if (jack) {
        return jack->outgoing.size;
    }

    if (fuse()) report_bad_request();

    return 0;
}

size_t SOCKETS::read(size_t sid, void *buf, size_t count) noexcept {
    if (!count) return 0;

    JACK *const jack = find_jack(make_session_query(sid));

    if (!jack) {
        if (fuse()) report_bad_request();

        return 0;
    }

    if (jack->incoming.size == 0) {
        return 0;
    }

    count = std::min(count, jack->incoming.size);

    if (buf) {
        std::memcpy(buf, to_uint8(jack->incoming), count);
    }

    if (jack->incoming.size > count) {
        std::memmove(
            jack->incoming.data,
            to_uint8(jack->incoming) + count, jack->incoming.size - count
        );
    }

    jack->incoming.size -= count;
    to_char(jack->incoming)[jack->incoming.size] = '\0';

    return count;
}

const char *SOCKETS::read(size_t sid) noexcept {
    JACK *const jack = find_jack(make_session_query(sid));

    if (!jack || jack->incoming.size == 0) {
        return "";
    }

    const char *c_str = to_char(jack->incoming);
    jack->incoming.size = 0;

    return c_str;
}

const char *SOCKETS::peek(size_t sid) noexcept {
    JACK *const jack = find_jack(make_session_query(sid));

    if (!jack || jack->incoming.size == 0) {
        return "";
    }

    return to_char(jack->incoming);
}

SOCKETS::ERROR SOCKETS::write(
    size_t sid, const void *buf, size_t count
) noexcept {
    if (!buf) {
        return fuse() ? report_bad_request() : ERROR::BAD_REQUEST;
    }

    JACK *const jack = find_jack(make_session_query(sid));

    if (!jack) {
        return fuse() ? report_bad_request() : ERROR::BAD_REQUEST;
    }

    if (count) {
        const PIPE wrapper{
            make_pipe(reinterpret_cast<const uint8_t *>(buf), count)
        };

        ERROR error{ append(wrapper, jack->outgoing) };

        if (error != ERROR::NONE) {
            return (
                fuse() ? (
                    report(
                        make_session(sid, error, true),
                        "%s: %s", __FUNCTION__, to_string(error)
                    )
                ) : error
            );
        }

        set_event(*jack, EVENT::WRITE, jack->outgoing.size);
    }

    return ERROR::NONE;
}

SOCKETS::ERROR SOCKETS::write(size_t sid, const char *text) noexcept {
    return write(sid, text, std::strlen(text));
}

SOCKETS::ERROR SOCKETS::writef(size_t sid, const char *fmt, ...) noexcept {
    char stackbuf[1024];

    std::va_list args;
    va_start(args, fmt);
    int retval = vsnprintf(stackbuf, sizeof(stackbuf), fmt, args);
    va_end(args);

    if (retval < 0) {
        log(
            "%s: encoding error when formatting '%s' (%s:%d).",
            __FUNCTION__, fmt, LEAF(__FILE__), __LINE__
        );

        return ERROR::BAD_REQUEST;
    }

    JACK &jack = get_jack(make_session_query(sid));

    if (static_cast<size_t>(retval) < sizeof(stackbuf)) {
        const PIPE wrapper{
            make_pipe(reinterpret_cast<const uint8_t *>(stackbuf), retval)
        };

        ERROR error{ append(wrapper, jack.outgoing) };

        if (error != ERROR::NONE) {
            return (
                fuse() ? (
                    report(
                        make_session(sid, error, true),
                        "%s: %s", __FUNCTION__, to_string(error)
                    )
                ) : error
            );
        }

        set_event(jack, EVENT::WRITE, jack.outgoing.size);

        return ERROR::NONE;
    }

    PIPE &buffer = jack.outgoing;

    size_t heapbuf_sz = static_cast<size_t>(retval) + 1;
    char *heapbuf = nullptr;

    ERROR error{ reserve(buffer, buffer.size + heapbuf_sz) };

    if (error != ERROR::NONE) {
        return (
            fuse() ? (
                report(
                    make_session(sid, error, true),
                    "%s: %s", __FUNCTION__, to_string(error)
                )
            ) : error
        );
    }

    heapbuf = static_cast<char *>(buffer.data) + buffer.size;

    va_start(args, fmt);
    retval = vsnprintf(heapbuf, heapbuf_sz, fmt, args);
    va_end(args);

    if (retval < 0) {
        log(
            "%s: encoding error when formatting '%s' (%s:%d).",
            __FUNCTION__, fmt, LEAF(__FILE__), __LINE__
        );

        return ERROR::BAD_REQUEST;
    }
    else if (static_cast<size_t>(retval) < heapbuf_sz) {
        buffer.size += retval;
        set_event(jack, EVENT::WRITE, jack.outgoing.size);

        return ERROR::NONE;
    }

    return report_bug();
}

const SOCKETS::RESULT &SOCKETS::report(const RESULT &result) noexcept {
    if (result.error != ERROR::NONE) {
        report(
            result.error, result.code, result.call, result.text, result.file,
            result.line
        );
    }

    return result;
}

const SOCKETS::RESULT &SOCKETS::report(
    size_t session_id, const RESULT &result
) noexcept {
    if (!result.error) {
        if (result.text[0]) {
            report(
                make_session(session_id, result.error, session_id),
                "%s", result.text
            );
        }
    }
    else if (result.error == ERROR::LIBRARY || result.error == ERROR::UNKNOWN) {
        report(
            make_session(session_id, result.error, session_id),
            "%s: %d: %s (%s:%d)",
            result.call, result.code, result.text, result.file, result.line
        );
    }
    else {
        report(
            make_session(session_id, result.error, session_id),
            "%s: %d: %s", result.call, result.code, result.text
        );
    }

    return result;
}

const SOCKETS::RESULT &SOCKETS::report(
    const JACK *jack, const RESULT &result
) noexcept {
    return report(jack ? jack->id : 0, result);
}

SOCKETS::ERROR SOCKETS::report(
    SOCKETS::SESSION session, const char *fmt, ...
) const noexcept {
    char stackbuf[256];
    char *bufptr = stackbuf;
    size_t bufsz = sizeof(stackbuf);

    sigset_t sigset_orig;

    call_pthread_sigmask(SIG_SETMASK, &sigset_all, &sigset_orig);

    for (size_t i=0; i<2 && bufptr; ++i) {
        std::va_list args;
        va_start(args, fmt);
        int cx = vsnprintf(bufptr, bufsz, fmt, args);
        va_end(args);

        if ((cx >= 0 && (size_t)cx < bufsz) || cx < 0) {
            if (log_callback) {
                log_callback(session, bufptr);
            }
            else {
                if (::write(STDERR_FILENO, bufptr, strlen(bufptr)) > 0) {
                    ::write(STDERR_FILENO, "\n", 1);
                }
            }

            break;
        }

        if (bufptr == stackbuf) {
            bufsz = cx + 1;
            bufptr = new (std::nothrow) char[bufsz];

            if (!bufptr) {
                static constexpr const char *const OOM = "Out Of Memory!";

                if (log_callback) {
                    log_callback(session, OOM);
                }
                else {
                    if (::write(STDERR_FILENO, OOM, strlen(OOM)) > 0) {
                        ::write(STDERR_FILENO, "\n", 1);
                    }
                }
            }
        }
        else {
            if (log_callback) {
                log_callback(session, bufptr);
            }
            else {
                if (::write(STDERR_FILENO, bufptr, strlen(bufptr)) > 0) {
                    ::write(STDERR_FILENO, "\n", 1);
                }
            }

            break;
        }
    }

    call_pthread_sigmask(SIG_SETMASK, &sigset_orig, nullptr);

    if (bufptr && bufptr != stackbuf) delete [] bufptr;

    return session.error;
}

SOCKETS::ERROR SOCKETS::report(
    ERROR error, int line, const char *file, char const *function
) const noexcept {
    return report(
        make_session(error),
        "%s: %s (%s:%d)", function, to_string(error), file, line
    );
}

SOCKETS::ERROR SOCKETS::report(
    ERROR error, int code, char const *function, const char *message,
    const char *file, int line
) const noexcept {
    return report(
        make_session(error),
        "%s: %d: %s (%s:%d)", function, code, message, file, line
    );
}

SOCKETS::ERROR SOCKETS::report_bug(
    const char *comment, const char *file, int line
) const noexcept {
    if (!comment) {
        comment = "forbidden condition met";
    }

    return report(
        make_session(ERROR::LIBRARY), "%s (%s:%d)", comment, file, line
    );
}

SOCKETS::ERROR SOCKETS::report_bad_request(
    const char *comment, const char *file, int line
) const noexcept {
    if (!comment) {
        comment = "invalid request received from caller";
    }

    return report(
        make_session(ERROR::BAD_REQUEST), "%s (%s:%d)", comment, file, line
    );
}

SOCKETS::ERROR SOCKETS::report_memory_exhaustion(
    const char *comment, const char *file, int line
) const noexcept {
    if (!comment) {
        comment = "Out Of Memory";
    }

    return report(
        make_session(ERROR::OUT_OF_MEMORY), "%s (%s:%d)", comment, file, line
    );
}

bool SOCKETS::fuse(const char *file, int line) const noexcept {
    size_t i = (static_cast<size_t>(line) / BITS_PER_BYTE) % sizeof(fuses);

    switch (line % BITS_PER_BYTE) {
        case 0: if (fuses[i] & (1<<0)) return false; fuses[i] |= (1<<0); break;
        case 1: if (fuses[i] & (1<<1)) return false; fuses[i] |= (1<<1); break;
        case 2: if (fuses[i] & (1<<2)) return false; fuses[i] |= (1<<2); break;
        case 3: if (fuses[i] & (1<<3)) return false; fuses[i] |= (1<<3); break;
        case 4: if (fuses[i] & (1<<4)) return false; fuses[i] |= (1<<4); break;
        case 5: if (fuses[i] & (1<<5)) return false; fuses[i] |= (1<<5); break;
        case 6: if (fuses[i] & (1<<6)) return false; fuses[i] |= (1<<6); break;
        case 7: if (fuses[i] & (1<<7)) return false; fuses[i] |= (1<<7); break;
    }

    log("fuse blows in %s on line %d", file, line);

    return true;
}

void SOCKETS::die(const char *file, int line) const noexcept {
    report_bug("fatal error", file, line);
    fflush(nullptr);
    std::abort();
}

SOCKETS::ERROR SOCKETS::handle_close(JACK &jack) noexcept {
    if (jack.bitset.reconnect) {
        SESSION new_session{
            connect(
                to_char(jack.host), to_char(jack.port),
                jack.ai_family, jack.ai_flags, &jack
            )
        };

        if (!new_session) {
            if (new_session.error == ERROR::OUT_OF_MEMORY) {
                // We postpone handling this event because we may get more
                // memory soon.

                return new_session.error;
            }

            jack.bitset.reconnect = false;
        }
        else {
            // Let's swap the session IDs of these jacks so that the original
            // session ID would prevail.

            JACK &new_jack = get_jack(make_session_query(new_session));

            if (!swap_sessions(&jack, &new_jack)) {
                close_and_release(new_jack);
                jack.bitset.reconnect = false;
            }
        }
    }

    rem_event(jack, EVENT::CLOSE);

    if (jack.bitset.connecting
    && !jack.bitset.reconnect
    && !has_event(jack, EVENT::DISCONNECTION)) {
        // Here we postpone closing this descriptor because we first want to
        // notify the user of this library of the connection that could not be
        // established.

        jack.bitset.connecting = false;
        set_event(jack, EVENT::DISCONNECTION);

        return ERROR::NONE;
    }

    close_and_release(jack);

    return ERROR::NONE;
}

SOCKETS::ERROR SOCKETS::handle_epoll(
    JACK &epoll_jack, int timeout
) noexcept {
    static constexpr const EVENT blockers[]{
        EVENT::CONNECTION,
        EVENT::DISCONNECTION,
        EVENT::INCOMING
    };

    for (EVENT event_type : blockers) {
        if (!find_jack(make_event_query(event_type))) {
            continue;
        }

        if (!bitset.alerted) {
            bitset.alerted = true;

            return ERROR::NONE;
        }

        return report(
            make_session(ERROR::PENDING_ALERT),
            "%s", "cannot serve sockets if there are unhandled events"
        );
    }

    bitset.alerted = false;

    static constexpr const unsigned int max_int{
        static_cast<unsigned int>(std::numeric_limits<int>::max())
    };

    int maxevents{
        epoll_jack.epoll_ev.size > max_int ? (
            std::numeric_limits<int>::max()
        ) : static_cast<int>(epoll_jack.epoll_ev.size)
    };

    RESULT result = report(
        call_epoll_pwait(
            epoll_jack.descriptor, to_epoll_event(epoll_jack.epoll_ev),
            maxevents, timeout, &sigset_none
        )
    );

    if (!result) {
        return result.error;
    }

    int pending = result.value;

    if (pending == 0) {
        bitset.timeout = true;
    }
    else if (pending == maxevents) {
        // Increase the epoll event buffer size.
        size_t old_size = epoll_jack.epoll_ev.size;

        ERROR error{
            insert(
                epoll_jack.epoll_ev, make_pipe_entry(PIPE::TYPE::EPOLL_EVENT)
            )
        };

        epoll_jack.epoll_ev.size = epoll_jack.epoll_ev.capacity;

        size_t new_size = epoll_jack.epoll_ev.size;

        if (error != ERROR::NONE
        &&  error != ERROR::OUT_OF_MEMORY
        &&  fuse()) {
            report(error);
        }

        if (old_size != new_size) {
            log("changed epoll event buffer size to %lu", new_size);
        }
    }

    epoll_event *events = to_epoll_event(epoll_jack.epoll_ev);

    for (int i=0; i<pending; ++i) {
        const int d = events[i].data.fd;
        JACK &jack = get_jack(make_descriptor_query(d));

        if ((  events[i].events & EPOLLERR )
        ||  (  events[i].events & EPOLLHUP )
        ||  (  events[i].events & EPOLLRDHUP )
        ||  (!(events[i].events & (EPOLLIN|EPOLLOUT) ))) {
            int socket_error = 0;
            socklen_t socket_errlen = sizeof(socket_error);

            if (events[i].events & EPOLLERR) {
                RESULT result = report(
                    call_getsockopt(
                        d, SOL_SOCKET, SO_ERROR,
                        static_cast<void*>(&socket_error), &socket_errlen
                    )
                );

                if (!result.error) {
                    switch (socket_error) {
                        case ENETUNREACH:
                        case ETIMEDOUT:
                        case ECONNREFUSED: {
                            if (jack.bitset.connecting) {
                                jack.bitset.reconnect = true;
                            }
                            [[fallthrough]];
                        }
                        case EPIPE:
                        case ECONNRESET: {
                            report(
                                make_session(jack.id),
                                "%s", strerror(socket_error)
                            );

                            break;
                        }
                        default: {
                            report(
                                make_session(jack.id, ERROR::LIBRARY, true),
                                "%s (%s:%d)",
                                strerror(socket_error), LEAF(__FILE__), __LINE__
                            );

                            break;
                        }
                    }
                }
            }
            else if ((events[i].events & EPOLLHUP) == false
            && (events[i].events & EPOLLRDHUP) == false) {
                log(
                    "unexpected events %lu on descriptor %d (%s:%d)",
                    static_cast<long unsigned>(events[i].events), d,
                    LEAF(__FILE__), __LINE__
                );
            }

            jack.bitset.may_shutdown = false;
            terminate(d);

            continue;
        }

        if (is_listener(jack)) {
            set_event(jack, EVENT::ACCEPT);
        }
        else {
            if (events[i].events & EPOLLIN) {
                set_event(jack, EVENT::READ);
            }

            if (events[i].events & EPOLLOUT) {
                set_event(jack, EVENT::WRITE);

                if (jack.bitset.connecting) {
                    jack.bitset.connecting = false;
                    set_event(jack, EVENT::CONNECTION);
                    jack.bitset.may_shutdown = true;

                    ERROR error{
                        modify_epoll(d, EPOLLIN|EPOLLET|EPOLLRDHUP)
                    };

                    if (error != ERROR::NONE) {
                        terminate(d);
                    }
                }
            }
        }
    }

    return ERROR::NONE;
}

SOCKETS::ERROR SOCKETS::handle_read(JACK &jack) noexcept {
    static constexpr const size_t padding = 1;
    PIPE &buffer = jack.incoming;

    if (!buffer.capacity) {
        ERROR error{ reserve(buffer, 1 + padding) };

        if (error != ERROR::NONE) {
            return report(error);
        }
    }

    const int descriptor = jack.descriptor;
    size_t intake_left = std::numeric_limits<size_t>::max() - jack.intake;
    intake_left -= std::min(intake_left, padding);
    const size_t intake = std::numeric_limits<size_t>::max() - intake_left;

    for (size_t total_count = 0;;) {
        ssize_t count;
        char *const buf = to_char(buffer) + buffer.size;
        size_t buf_sz = buffer.capacity - buffer.size;
        buf_sz = (padding < buf_sz ? buf_sz - padding : 0);

        if (!buf_sz) {
            return ERROR::NONE;
        }
        else if ((count = ::read(descriptor, buf, buf_sz)) < 0) {
            if (count == -1) {
                int code = errno;

                if (code == EAGAIN || code == EWOULDBLOCK) {
                    rem_event(jack, EVENT::READ);

                    return ERROR::NONE;
                }
                else {
                    switch (code) {
                        default: {
                            log( // Unusual errors are logged.
                                "read: %s (%s:%d)", strerror(code),
                                LEAF(__FILE__), __LINE__
                            );
                            [[fallthrough]];
                        }
                        case EPIPE:
                        case ECONNRESET: {
                            break;
                        }
                        case EINTR: {
                            // This should never happen to non-blocking sockets.
                            break;
                        }
                    }
                }

                break;
            }
            else if (fuse()) {
                log(
                    "read(%d, ?, %lu): unexpected return value %lld (%s:%d)",
                    descriptor, buf_sz, static_cast<long long>(count),
                    LEAF(__FILE__), __LINE__
                );
            }

            break;
        }
        else if (count == 0) {
            // End of file. The remote has closed the connection.
            break;
        }

        if (!total_count) {
            set_event(jack, EVENT::INCOMING);
        }

        total_count += count;
        buffer.size += count;
        buf[count] = '\0'; // Null terminator is needed for reading convenience.

        if (buffer.size + padding == buffer.capacity
        &&  buffer.capacity < intake) {
            ERROR error{
                // Errors here are not fatal because we are just trying to
                // increase the capacity of the buffer.

                reserve(buffer, std::min(2 * buffer.capacity, intake))
            };

            if (error != ERROR::NONE
            &&  error != ERROR::OUT_OF_MEMORY) {
                report(error);
            }
        }
    }

    rem_event(jack, EVENT::READ);
    jack.bitset.may_shutdown = false;
    terminate(descriptor);

    return ERROR::NONE;
}

SOCKETS::ERROR SOCKETS::handle_write(JACK &jack) noexcept {
    int descriptor = jack.descriptor;
    PIPE &outgoing = jack.outgoing;

    if (outgoing.size == 0) {
        rem_event(jack, EVENT::WRITE);
        return ERROR::NONE;
    }

    const unsigned char *const bytes = to_uint8(outgoing);
    size_t length = outgoing.size;

    bool try_again_later = true;
    size_t istart;
    ssize_t nwrite;

    for (istart = 0; istart<length; istart+=nwrite) {
        nwrite = send(
            descriptor, bytes+istart, length-istart, MSG_NOSIGNAL|MSG_DONTWAIT
        );

        if (nwrite < 0) {
            int code = errno;

            if (code == EAGAIN || code == EWOULDBLOCK) {
                // Let's start expecting EPOLLOUT.
                try_again_later = false;
            }
            else {
                switch (code) {
                    default: {
                        log( // Unusual errors are logged.
                            "send: %s (%s:%d)", strerror(code),
                            LEAF(__FILE__), __LINE__
                        );
                        [[fallthrough]];
                    }
                    case EPIPE:
                    case ECONNRESET: {
                        try_again_later = false;

                        break;
                    }
                    case EINTR: {
                        // This should never happen to non-blocking sockets.
                        break;
                    }
                }
            }

            break;
        }
        else if (nwrite == 0) {
            break;
        }
    }

    if (istart == length) {
        outgoing.size = 0;
    }
    else if (istart > outgoing.size) die();
    else if (istart > 0) {
        size_t new_size = outgoing.size - istart;

        std::memmove(outgoing.data, to_uint8(outgoing) + istart, new_size);

        outgoing.size = new_size;
    }

    if (try_again_later) {
        return modify_epoll(descriptor, EPOLLIN|EPOLLET|EPOLLRDHUP);
    }

    rem_event(jack, EVENT::WRITE);

    return modify_epoll(descriptor, EPOLLIN|EPOLLOUT|EPOLLET|EPOLLRDHUP);
}

SOCKETS::ERROR SOCKETS::handle_accept(JACK &jack) noexcept {
    // New incoming connection detected.
    const int descriptor = jack.descriptor;
    struct sockaddr in_addr;
    socklen_t in_len = sizeof(in_addr);

    RESULT result{
        call_accept4(
            descriptor, &in_addr, &in_len, SOCK_CLOEXEC|SOCK_NONBLOCK
        )
    };

    if (!result) {
        if (!result.error) {
            if (result.code == EAGAIN || result.code == EWOULDBLOCK) {
                rem_event(jack, EVENT::ACCEPT);
            }
            else report(jack.id, result);
        }
        else {
            if (result.error == ERROR::LIBRARY) {
                rem_event(jack, EVENT::ACCEPT);
            }

            report(jack.id, result);
        }

        return result.error;
    }

    const int client_descriptor = result.value;
    SESSION session{capture(make_jack(client_descriptor, descriptor))};

    if (!session.valid) {
        if (session.error == ERROR::LIBRARY) {
            report(session.error);
        }

        close_descriptor(client_descriptor);

        return (
            fuse() ? (
                report(
                    make_session(jack.id, session.error, true),
                    "%s: %s", __FUNCTION__, to_string(session.error)
                )
            ) : session.error
        );
    }

    JACK &client_jack = get_jack(make_descriptor_query(client_descriptor));
    char host[NI_MAXHOST];
    char port[NI_MAXSERV];

    result = report(
        &jack, call_getnameinfo(
            &in_addr, in_len,
            host, socklen_t(std::extent<decltype(host)>::value),
            port, socklen_t(std::extent<decltype(port)>::value),
            NI_NUMERICHOST|NI_NUMERICSERV
        )
    );

    if (!result || result.value != 0) {
        close_and_release(client_jack);

        return result.error;
    }

    const PIPE host_wrapper{
        make_pipe(
            reinterpret_cast<const uint8_t *>(host), std::strlen(host) + 1
        )
    };

    {
        ERROR error{ copy(host_wrapper, client_jack.host) };

        if (error != ERROR::NONE) {
            close_and_release(client_jack);

            return error;
        }
    }

    const PIPE port_wrapper{
        make_pipe(
            reinterpret_cast<const uint8_t *>(port), std::strlen(port) + 1
        )
    };

    {
        ERROR error{ copy(port_wrapper, client_jack.port) };

        if (error != ERROR::NONE) {
            close_and_release(client_jack);

            return error;
        }
    }

    {
        ERROR error{ bind_to_epoll(client_descriptor) };

        if (error != ERROR::NONE) {
            close_and_release(client_jack);

            return error;
        }
    }

    set_event(client_jack, EVENT::CONNECTION);
    client_jack.bitset.may_shutdown = true;

    const char *pfx{ in_addr.sa_family == AF_INET6 ? "[" : "" };
    const char *sfx{ in_addr.sa_family == AF_INET6 ? "]" : "" };

    report(
        make_session(client_jack.id),
        "new socket: %s%s%s:%s", pfx, host, sfx, port
    );

    // We successfully accepted one client, but since there may be more of
    // them waiting we should recursively retry until we fail to accept any
    // new connections.

    return handle_accept(jack);
}

SOCKETS::SESSION SOCKETS::connect(
    const char *host, const char *port, int ai_family, int ai_flags,
    JACK *predecessor, const char *file, int line
) noexcept {
    SESSION session{
        open_and_capture(
            host, port, ai_family, ai_flags, {}, predecessor, file, line
        )
    };

    if (!session) {
        return session;
    }

    JACK &new_jack = get_jack(make_session_query(session));
    const int descriptor = new_jack.descriptor;

    const PIPE host_wrapper{
        make_pipe(
            reinterpret_cast<const uint8_t *>(host), std::strlen(host) + 1
        )
    };

    {
        ERROR error{ copy(host_wrapper, new_jack.host) };

        if (error != ERROR::NONE) {
            close_and_release(new_jack);

            return make_session(error);
        }
    }

    const PIPE port_wrapper{
        make_pipe(
            reinterpret_cast<const uint8_t *>(port), std::strlen(port) + 1
        )
    };

    {
        ERROR error{ copy(port_wrapper, new_jack.port) };

        if (error != ERROR::NONE) {
            close_and_release(new_jack);

            return make_session(error);
        }
    }

    {
        // Let's block all signals before calling connect because we
        // don't want it to fail due to getting interrupted by a singal.

        sigset_t sigset_orig;
        ERROR error { block_signals(sigset_orig) };

        if (!error && (new_jack.blacklist || !report_bug())) {
            RESULT result{
                call_connect(
                    descriptor,
                    new_jack.blacklist->ai_addr,
                    new_jack.blacklist->ai_addrlen
                )
            };

            unblock_signals(sigset_orig);

            if (!result) {
                if (!result.error
                && (result.code == EAGAIN || result.code == EINPROGRESS)) {
                    new_jack.bitset.connecting = true;
                }
                else {
                    error = report(
                        predecessor ? predecessor : &new_jack, result
                    ).error;

                    if (swap_sessions(predecessor, &new_jack)) {
                        session = connect(
                            host, port, ai_family, ai_flags, &new_jack
                        );

                        swap_sessions(predecessor, &new_jack);
                    }
                    else session = make_session(error);

                    close_and_release(new_jack);

                    return session;
                }
            }
        }
        else {
            close_and_release(new_jack);

            return make_session(error);
        }
    }

    {
        ERROR error{ bind_to_epoll(descriptor) };

        if (error != ERROR::NONE) {
            close_and_release(new_jack);

            return make_session(error);
        }
    }

    if (new_jack.bitset.connecting) {
        ERROR error{ modify_epoll(descriptor, EPOLLOUT|EPOLLET) };

        if (error != ERROR::NONE) {
            close_and_release(new_jack);

            return make_session(error);
        }
    }
    else {
        new_jack.bitset.may_shutdown = true;
        set_event(new_jack, EVENT::CONNECTION);
    }

    return session;
}

void SOCKETS::terminate(int descriptor, const char *file, int line) noexcept {
    JACK &jack = get_jack(make_descriptor_query(descriptor));

    if (has_event(jack, EVENT::CLOSE)
    ||  has_event(jack, EVENT::DISCONNECTION)) {
        return;
    }

    if (jack.bitset.reconnect || jack.bitset.connecting) {
        set_event(jack, EVENT::CLOSE);
    }
    else {
        set_event(jack, EVENT::DISCONNECTION);
    }

    if (jack.bitset.may_shutdown) {
        jack.bitset.may_shutdown = false;

        if (has_event(jack, EVENT::WRITE) && !jack.bitset.connecting) {
            // Let's handle writing here so that the client would have a
            // chance to receive any pending bytes before being shut down.
            handle_write(jack);
        }

        report(jack.id, call_shutdown(descriptor, SHUT_WR));
    }

    if (is_listener(jack)) {
        for (size_t i=0; i<jack.children.size; ++i) {
            terminate(to_int(get_entry(jack.children, i)), file, line);
        }
    }
}

SOCKETS::SESSION SOCKETS::listen(
    const char *host, const char *port, int ai_family, int ai_flags,
    const std::initializer_list<int> options, JACK *predecessor,
    const char *file, int line
) noexcept {
    SESSION session{
        open_and_capture(host, port, ai_family, ai_flags, options, predecessor)
    };

    if (!session.valid) return session;

    JACK &new_jack = get_jack(make_session_query(session));
    const int descriptor = new_jack.descriptor;

    for (int option : options) {
        int optval = 1;

        RESULT result{
            call_setsockopt(
                descriptor, SOL_SOCKET, option,
                static_cast<const void *>(&optval), sizeof(optval)
            )
        };

        if (!result) {
            report(predecessor ? predecessor : &new_jack, result);
            close_and_release(new_jack);

            return make_session(result.error);
        }
    }

    {
        RESULT result{
            call_bind(
                descriptor,
                new_jack.blacklist->ai_addr, new_jack.blacklist->ai_addrlen
            )
        };

        if (!result) {
            report(predecessor ? predecessor : &new_jack, result);

            if (swap_sessions(predecessor, &new_jack)) {
                session = listen(
                    host, port, ai_family, ai_flags, options, &new_jack
                );

                swap_sessions(predecessor, &new_jack);
            }
            else session = make_session(result.error);

            close_and_release(new_jack);

            return session;
        }
    }

    RESULT result = report(
        predecessor ? predecessor : &new_jack,
        call_listen(descriptor, SOMAXCONN)
    );

    if (!result) {
        close_and_release(new_jack);

        return make_session(result.error);
    }

    {
        ERROR error{ bind_to_epoll(descriptor) };

        if (error != ERROR::NONE) {
            close_and_release(new_jack);

            return make_session(error);
        }
    }

    set_event(new_jack, EVENT::ACCEPT);
    new_jack.bitset.listener = true;

    struct sockaddr in_addr;
    socklen_t in_len = sizeof(in_addr);

    {
        RESULT result = report(
            predecessor ? predecessor : &new_jack,
            call_getsockname(
                descriptor, static_cast<struct sockaddr *>(&in_addr), &in_len
            )
        );

        if (!result) {
            return make_session(result.error);
        }
    }

    return session;
}

SOCKETS::SESSION SOCKETS::create_epoll() noexcept {
    RESULT result = report(call_epoll_create1(0));

    if (!result) {
        return make_session(result.error);
    };

    int epoll_descriptor = result.value;

    SESSION session{capture(make_jack(epoll_descriptor))};

    if (!session.valid) {
        report(session.error);
        close_descriptor(epoll_descriptor);

        return make_session(session.error);
    }

    JACK &jack = get_jack(make_descriptor_query(epoll_descriptor));

    {
        ERROR error{ reserve(jack.epoll_ev, 1) };

        if (error != ERROR::NONE) {
            close_and_release(jack);

            return make_session(report(error));
        }

        jack.epoll_ev.size = jack.epoll_ev.capacity;
    }

    set_event(jack, EVENT::EPOLL);

    return session;
}

SOCKETS::ERROR SOCKETS::operate_epoll(
    int operation, epoll_event event
) noexcept {
    JACK &epoll_jack = get_epoll_jack();

    RESULT result = report(
        call_epoll_ctl(
            epoll_jack.descriptor, operation, event.data.fd, &event
        )
    );

    if (!result) {
        return result.error;
    }

    return ERROR::NONE;
}

SOCKETS::ERROR SOCKETS::bind_to_epoll(
    int descriptor, const char *file, int line
) noexcept {
    ERROR error{
        operate_epoll(
            EPOLL_CTL_ADD,
            make_epoll_event(descriptor, EPOLLIN|EPOLLET|EPOLLRDHUP)
        )
    };

    if (error != ERROR::NONE) {
        report_bug(nullptr, file, line);
    }

    return error;
}

SOCKETS::ERROR SOCKETS::modify_epoll(
    int descriptor, uint32_t events, const char *file, int line
) noexcept {
    ERROR error{
        operate_epoll(EPOLL_CTL_MOD, make_epoll_event(descriptor, events))
    };

    if (error != ERROR::NONE) {
        report_bug(nullptr, file, line);
    }

    return error;
}

SOCKETS::ERROR SOCKETS::block_signals(sigset_t &sigset_orig) noexcept {
    return report(
        call_pthread_sigmask(SIG_SETMASK, &sigset_all, &sigset_orig)
    ).error;
}

SOCKETS::ERROR SOCKETS::unblock_signals(sigset_t &sigset_orig) noexcept {
    return report(
        call_pthread_sigmask(SIG_SETMASK, &sigset_orig, nullptr)
    ).error;
}

SOCKETS::SESSION SOCKETS::open_and_capture(
    const char *host, const char *port, int ai_family, int ai_flags,
    const std::initializer_list<int> options, JACK *predecessor,
    const char *file, int line
) noexcept {
    static constexpr const bool establish_nonblocking_connections = true;
    const bool accept_incoming_connections = host == nullptr;

    struct addrinfo hint{ make_addrinfo(ai_family, ai_flags) };
    struct addrinfo *info = nullptr;
    struct addrinfo *addr = nullptr;
    struct addrinfo *prev = nullptr;

    ERROR error{
        report(predecessor, call_getaddrinfo(host, port, &hint, &info)).error
    };

    int descriptor = -1;

    if (!error) {
        for (addr = info; addr; prev = addr, addr = addr->ai_next) {
            if (predecessor && is_listed(*addr, predecessor->blacklist)) {
                continue;
            }

            RESULT result{
                call_socket(
                    addr->ai_family,
                    accept_incoming_connections ? (
                        addr->ai_socktype|SOCK_NONBLOCK|SOCK_CLOEXEC
                    ) : (
                        addr->ai_socktype|SOCK_CLOEXEC|(
                            establish_nonblocking_connections ? (
                                SOCK_NONBLOCK
                            ) : 0
                        )
                    ),
                    addr->ai_protocol
                )
            };

            if (!result) {
                error = report(predecessor, result).error;

                continue;
            }

            descriptor = result.value;

            break;
        }
    }

    SESSION session = make_session(error);

    do {
        if (!is_descriptor(descriptor)) {
            break;
        }

        char next_host[NI_MAXHOST];
        char next_port[NI_MAXSERV];

        RESULT result = report(
            predecessor, call_getnameinfo(
                addr->ai_addr, addr->ai_addrlen,
                next_host, socklen_t(std::extent<decltype(next_host)>::value),
                next_port, socklen_t(std::extent<decltype(next_port)>::value),
                NI_NUMERICHOST|NI_NUMERICSERV
            )
        );

        session = make_session(result.error);

        if (!result || result.value != 0
        || !(session = capture(make_jack(descriptor)))) {
            close_descriptor(descriptor);

            if (session.error != ERROR::OUT_OF_MEMORY) {
                report(session.error);
            }

            break;
        }

        JACK *new_jack = &get_jack(make_descriptor_query(descriptor));

        const PIPE
        host_wrapper{
            make_pipe(
                reinterpret_cast<const uint8_t *>(next_host),
                std::strlen(next_host) + 1
            )
        },
        port_wrapper{
            make_pipe(
                reinterpret_cast<const uint8_t *>(next_port),
                std::strlen(next_port) + 1
            )
        };

        ERROR host_error{ copy(host_wrapper, new_jack->host) };
        ERROR port_error{ copy(port_wrapper, new_jack->port) };

        if (host_error != ERROR::NONE || port_error != ERROR::NONE) {
            close_and_release(*new_jack);
            session = make_session(!host_error ? port_error : host_error);

            break;
        }

        new_jack->ai_family = ai_family;
        new_jack->ai_flags  = ai_flags;

        if (new_jack->blacklist) {
            report_bug();
            freeaddrinfo(new_jack->blacklist);
            new_jack->blacklist = nullptr;
        }

        new_jack->blacklist = info;
        info = addr->ai_next;
        addr->ai_next = nullptr;

        if (prev) {
            // Make sure that the active addrinfo is the first one that appears
            // in the blacklist.

            prev->ai_next = nullptr;
            addr->ai_next = new_jack->blacklist;
            new_jack->blacklist = addr;
        }

        const char *pfx{ addr->ai_family == AF_INET6 ? "[" : "" };
        const char *sfx{ addr->ai_family == AF_INET6 ? "]" : "" };

        report(
            make_session(predecessor ? predecessor->id : new_jack->id),
            "new socket: %s%s%s:%s", pfx, next_host, sfx, next_port
        );
    }
    while (false);

    if (info) {
        freeaddrinfo(info);
    }

    return session;
}

void SOCKETS::close_descriptor(int descriptor) noexcept {
    // Let's block all signals before calling close because we don't
    // want it to fail due to getting interrupted by a singal.

    sigset_t sigset_orig;

    if (block_signals(sigset_orig) != ERROR::NONE) {
        return die();
    }

    if (call_close(descriptor).code == EINTR && fuse()) {
        report_bug();
    }

    if (unblock_signals(sigset_orig) != ERROR::NONE) {
        return die();
    }
}

void SOCKETS::close_and_release(
    JACK &jack, const char *file, int line
) noexcept {
    // Let's block all signals before calling close because we don't
    // want it to fail due to getting interrupted by a singal.

    sigset_t sigset_orig;

    if (block_signals(sigset_orig) != ERROR::NONE) {
        return die();
    }

    JACK *last = &jack;

    for (;;) {
        JACK *next_last{
            last->children.size ? (
                find_jack(
                    make_descriptor_query(to_int(get_last(last->children)))
                )
            ) : nullptr
        };

        if (next_last) {
            last = next_last;
            continue;
        }

        if (call_close(last->descriptor).code == EINTR && fuse()) {
            report_bug();
        }

        release(last);

        if (last == &jack) {
            break;
        }

        last = &jack;
    }

    if (unblock_signals(sigset_orig) != ERROR::NONE) {
        return die();
    }
}

SOCKETS::SESSION SOCKETS::capture(const JACK &copy) noexcept {
    if (last_jack_id == std::numeric_limits<decltype(last_jack_id)>::max()) {
        // It appears as if we have ran out of session IDs. In practice this
        // should never happen but if it does, we log it once. The library will
        // be unable to create any more sessions.

        return make_session(
            fuse() ? (
                report(
                    make_session(ERROR::LIBRARY),
                    "%s", "session ID pool exhausted"
                )
            ) : ERROR::LIBRARY
        );
    }

    if (!is_descriptor(copy.descriptor)) {
        return make_session(report_bug());
    }

    for (auto &ev : copy.event_lookup) {
        ERROR error{
            reserve(
                INDEX::TYPE::EVENT_DESCRIPTOR,
                make_key(static_cast<EVENT>(&ev - &(copy.event_lookup[0]))),
                jack_count + 1
            )
        };

        if (!error) {
            continue;
        }

        return make_session(error);
    }

    PIPE *siblings = nullptr;

    if (is_descriptor(copy.parent.descriptor)) {
        siblings = &get_jack(
            make_descriptor_query(copy.parent.descriptor)
        ).children;

        ERROR error{ insert(*siblings, make_pipe_entry(copy.descriptor)) };

        if (error != ERROR::NONE) {
            return make_session(error);
        }
    }

    int descriptor = copy.descriptor;
    JACK *const jack = new_jack(&copy);

    if (!jack) {
        if (siblings) {
            pop_back(*siblings);
        }

        return make_session(ERROR::OUT_OF_MEMORY);
    }

    if (siblings) {
        if (!siblings->size) {
            die();
        }

        size_t index = siblings->size - 1;
        static constexpr const size_t max_index{
            std::numeric_limits<decltype(jack->parent.child_index)>::max()
        };

        if (index > max_index) {
            die();
        }

        jack->parent.child_index = (
            static_cast<decltype(jack->parent.child_index)>(index)
        );
    }

    jack->id = last_jack_id + 1;

    ERROR error = ERROR::NONE;

    for (;;) {
        {
            INDEX::ENTRY entry{
                insert(
                    INDEX::TYPE::SESSION_JACK,
                    make_key(jack->id), make_pipe_entry(jack)
                )
            };

            if (!entry.valid) {
                error = entry.error;
                break;
            }
        }

        {
            INDEX::ENTRY entry{
                insert(
                    INDEX::TYPE::DESCRIPTOR_JACK,
                    make_key(descriptor), make_pipe_entry(jack)
                )
            };

            if (!entry.valid) {
                error = entry.error;
                break;
            }
        }

        break;
    }

    if (!error) {
        ++jack_count;

        return make_session( (last_jack_id = jack->id) );
    }

    release(jack);

    return make_session(error);
}

void SOCKETS::release(JACK *jack) noexcept {
    if (!jack) {
        report_bug();
        return;
    }

    for (auto &ev : jack->event_lookup) {
        rem_event(*jack, static_cast<EVENT>(&ev - &(jack->event_lookup[0])));
    }

    if (is_descriptor(jack->parent.descriptor)) {
        rem_child(
            get_jack(make_descriptor_query(jack->parent.descriptor)), *jack
        );
    }

    if (jack->children.size) {
        report_bug(); // Children should have been already released.
    }

    destroy(jack->epoll_ev);
    destroy(jack->children);
    destroy(jack->incoming);
    destroy(jack->outgoing);
    destroy(jack->host);
    destroy(jack->port);

    if (jack->blacklist) {
        freeaddrinfo(jack->blacklist);
    }

    if (erase(INDEX::TYPE::DESCRIPTOR_JACK, make_key(jack->descriptor))) {
        --jack_count;
    }

    erase(INDEX::TYPE::SESSION_JACK, make_key(jack->id));

    recycle(get_memory(jack));
}

SOCKETS::JACK *SOCKETS::find_jack(const QUERY &query) const noexcept {
    INDEX::ENTRY entry;

    switch (query.type) {
        case QUERY::TYPE::DESCRIPTOR: {
            entry = find(
                INDEX::TYPE::DESCRIPTOR_JACK, make_key(query.descriptor)
            );

            break;
        }
        case QUERY::TYPE::SESSION: {
            entry = find(
                INDEX::TYPE::SESSION_JACK, make_key(query.session)
            );

            break;
        }
        case QUERY::TYPE::EVENT: {
            entry = find(
                INDEX::TYPE::EVENT_DESCRIPTOR, make_key(query.event)
            );

            if (entry.valid) {
                return find_jack(
                    make_descriptor_query(to_int(get_value(entry)))
                );
            }
            else return nullptr;
        }
    }

    if (!entry.valid) {
        return nullptr;
    }

    return to_jack(get_entry(*entry.val_pipe, entry.index));
}

SOCKETS::JACK *SOCKETS::find_epoll_jack() const noexcept {
    return find_jack(make_event_query(EVENT::EPOLL));
}

SOCKETS::JACK &SOCKETS::get_jack(const QUERY &query) const noexcept {
    JACK *const jack = find_jack(query);

    if (!jack) die();

    return *jack;
}

SOCKETS::JACK &SOCKETS::get_epoll_jack() const noexcept {
    JACK *const rec = find_epoll_jack();

    if (!rec) die();

    return *rec;
}

const SOCKETS::PIPE *SOCKETS::find_descriptors(EVENT ev) const noexcept {
    INDEX::ENTRY entry{find(INDEX::TYPE::EVENT_DESCRIPTOR, make_key(ev))};

    if (entry.valid) {
        return entry.val_pipe;
    }

    return nullptr;
}

SOCKETS::PIPE &SOCKETS::get_buffer(BUFFER buffer) noexcept {
    size_t index = static_cast<size_t>(buffer);

    if (index >= std::extent<decltype(buffers)>::value) die();

    return buffers[index];
}

SOCKETS::INDEX &SOCKETS::get_index(INDEX::TYPE index_type) noexcept {
    size_t i = static_cast<size_t>(index_type);

    if (i >= std::extent<decltype(indices)>::value) die();

    return indices[i];
}

SOCKETS::JACK *SOCKETS::to_jack(PIPE::ENTRY entry) const noexcept {
    if (entry.type != PIPE::TYPE::JACK_PTR) die();

    return static_cast<JACK *>(entry.as_ptr);
}

SOCKETS::MEMORY *SOCKETS::to_memory(PIPE::ENTRY entry) const noexcept {
    if (entry.type != PIPE::TYPE::MEMORY_PTR) die();

    return static_cast<MEMORY *>(entry.as_ptr);
}

int SOCKETS::to_int(PIPE::ENTRY entry) const noexcept {
    if (entry.type != PIPE::TYPE::INT) die();

    return entry.as_int;
}

uint64_t SOCKETS::to_uint64(PIPE::ENTRY entry) const noexcept {
    if (entry.type != PIPE::TYPE::UINT64) die();

    return entry.as_uint64;
}

SOCKETS::KEY SOCKETS::to_key(PIPE::ENTRY entry) const noexcept {
    if (entry.type != PIPE::TYPE::KEY) die();

    return entry.as_key;
}

int *SOCKETS::to_int(const PIPE &pipe) const noexcept {
    if (pipe.type != PIPE::TYPE::INT) die();

    return static_cast<int *>(pipe.data);
}

uint8_t *SOCKETS::to_uint8(const PIPE &pipe) const noexcept {
    if (pipe.type != PIPE::TYPE::UINT8) die();

    return static_cast<uint8_t *>(pipe.data);
}

char *SOCKETS::to_char(const PIPE &pipe) const noexcept {
    if (pipe.type != PIPE::TYPE::UINT8) die();

    return static_cast<char *>(pipe.data);
}

uint64_t *SOCKETS::to_uint64(const PIPE &pipe) const noexcept {
    if (pipe.type != PIPE::TYPE::UINT64) die();

    return static_cast<uint64_t *>(pipe.data);
}

SOCKETS::KEY *SOCKETS::to_key(const PIPE &pipe) const noexcept {
    if (pipe.type != PIPE::TYPE::KEY) die();

    return static_cast<KEY *>(pipe.data);
}

epoll_event *SOCKETS::to_epoll_event(const PIPE &pipe) const noexcept {
    if (pipe.type != PIPE::TYPE::EPOLL_EVENT) die();

    return static_cast<epoll_event *>(pipe.data);
}

void **SOCKETS::to_ptr(const PIPE &pipe) const noexcept {
    switch (pipe.type) {
        case PIPE::TYPE::PTR:
        case PIPE::TYPE::MEMORY_PTR:
        case PIPE::TYPE::JACK_PTR: {
            return static_cast<void **>(pipe.data);
        }
        case PIPE::TYPE::UINT8:
        case PIPE::TYPE::UINT64:
        case PIPE::TYPE::INT:
        case PIPE::TYPE::KEY:
        case PIPE::TYPE::EPOLL_EVENT:
        case PIPE::TYPE::NONE: {
            break;
        }
    }

    die();
}

void *SOCKETS::to_ptr(PIPE::ENTRY &entry) const noexcept {
    switch (entry.type) {
        case PIPE::TYPE::PTR:
        case PIPE::TYPE::MEMORY_PTR:
        case PIPE::TYPE::JACK_PTR:    return &(entry.as_ptr);
        case PIPE::TYPE::UINT8:       return &(entry.as_uint8);
        case PIPE::TYPE::UINT64:      return &(entry.as_uint64);
        case PIPE::TYPE::INT:         return &(entry.as_int);
        case PIPE::TYPE::KEY:         return &(entry.as_key);
        case PIPE::TYPE::EPOLL_EVENT: return &(entry.as_epoll_event);
        case PIPE::TYPE::NONE:        break;
    }

    die();
}

void *SOCKETS::to_ptr(PIPE &pipe, size_t index) const noexcept {
    switch (pipe.type) {
        case PIPE::TYPE::PTR:
        case PIPE::TYPE::MEMORY_PTR:
        case PIPE::TYPE::JACK_PTR:    return to_ptr(pipe) + index;
        case PIPE::TYPE::UINT8:       return to_uint8(pipe) + index;
        case PIPE::TYPE::UINT64:      return to_uint64(pipe) + index;
        case PIPE::TYPE::INT:         return to_int(pipe) + index;
        case PIPE::TYPE::KEY:         return to_key(pipe) + index;
        case PIPE::TYPE::EPOLL_EVENT: return to_epoll_event(pipe) + index;
        case PIPE::TYPE::NONE:        break;
    }

    die();
}

const void *SOCKETS::to_ptr(const PIPE &pipe, size_t index) const noexcept {
    return to_ptr(const_cast<PIPE&>(pipe), index);
}

void SOCKETS::set_event(JACK &jack, EVENT event, bool value) noexcept {
    if (value == false) {
        rem_event(jack, event);
        return;
    }

    size_t index = static_cast<size_t>(event);

    if (index >= std::extent<decltype(jack.event_lookup)>::value) {
        return die();
    }

    unsigned pos = jack.event_lookup[index];

    if (pos != std::numeric_limits<unsigned>::max()) {
        return; // Already set.
    }

    INDEX::ENTRY entry{
        insert(
            INDEX::TYPE::EVENT_DESCRIPTOR,
            make_key(event), make_pipe_entry(jack.descriptor)
        )
    };

    if (entry.valid) {
        if (entry.index >= std::numeric_limits<unsigned>::max()) {
            // the number of descriptors is limited by the UINT max.
            return die();
        }

        jack.event_lookup[index] = static_cast<unsigned>(entry.index);

        return;
    }

    report(entry.error);
    die();
}

void SOCKETS::rem_event(JACK &jack, EVENT event) noexcept {
    size_t index = static_cast<size_t>(event);

    if (index >= std::extent<decltype(jack.event_lookup)>::value) {
        return die();
    }

    unsigned pos = jack.event_lookup[index];

    if (pos == std::numeric_limits<unsigned>::max()) {
        return;
    }

    size_t erased = erase(
        INDEX::TYPE::EVENT_DESCRIPTOR,
        make_key(event), make_pipe_entry(jack.descriptor), pos, 1
    );

    if (!erased) {
        return die();
    }

    INDEX::ENTRY entry{
        find(INDEX::TYPE::EVENT_DESCRIPTOR, make_key(event), {}, pos, 1)
    };

    if (entry.valid && entry.index == pos) {
        int other_descriptor = to_int(get_value(entry));

        get_jack(
            make_descriptor_query(other_descriptor)
        ).event_lookup[index] = pos;
    }

    jack.event_lookup[index] = std::numeric_limits<unsigned>::max();
}

bool SOCKETS::has_event(const JACK &jack, EVENT event) const noexcept {
    size_t index = static_cast<size_t>(event);

    if (index >= std::extent<decltype(jack.event_lookup)>::value) {
        return false;
    }

    unsigned pos = jack.event_lookup[index];

    if (pos != std::numeric_limits<unsigned>::max()) {
        return true;
    }

    return false;
}

void SOCKETS::rem_child(JACK &jack, JACK &child) const noexcept {
    if (child.parent.child_index < 0
    || jack.descriptor != child.parent.descriptor) {
        return die();
    }

    const decltype(PIPE::size) index = child.parent.child_index;

    if (index + 1 == jack.children.size) {
        pop_back(jack.children);
    }
    else {
        PIPE::ENTRY entry { pop_back(jack.children) };
        replace(jack.children, index, entry);

        JACK &sibling = get_jack(make_descriptor_query(to_int(entry)));

        sibling.parent.child_index = (
            static_cast<decltype(sibling.parent.child_index)>(index)
        );
    }

    child.parent.descriptor = -1;
    child.parent.child_index = -1;
}

SOCKETS::INDEX::ENTRY SOCKETS::find(
    INDEX::TYPE index_type, KEY key, PIPE::ENTRY value,
    size_t start_i, size_t iterations
) const noexcept {
    const INDEX &index = indices[size_t(index_type)];

    if (index.buckets <= 0) die();

    INDEX::TABLE &table = index.table[key.value % index.buckets];
    PIPE &key_pipe = table.key;

    if (key_pipe.type != PIPE::TYPE::KEY) {
        die();
    }

    const KEY *const data = to_key(key_pipe);

    if (!data) {
        return {};
    }

    PIPE &val_pipe = table.value;

    if (value.type != PIPE::TYPE::NONE && value.type != val_pipe.type) {
        die();
    }

    size_t sz = key_pipe.size;
    size_t i = std::min(sz-1, start_i);

    for (; i<sz && iterations; --i, --iterations) {
        if (data[i].value != key.value) {
            continue;
        }

        if (value.type != PIPE::TYPE::NONE
        && std::memcmp(to_ptr(val_pipe, i), to_ptr(value), size(value.type))) {
            continue;
        }

        INDEX::ENTRY entry{};

        entry.index = i;
        entry.valid = true;
        entry.key_pipe = &key_pipe;
        entry.val_pipe = &val_pipe;

        return entry;
    }

    return {};
}

SOCKETS::ERROR SOCKETS::reserve(
    INDEX::TYPE index_type, KEY key, size_t capacity
) noexcept {
    ERROR error = ERROR::NONE;
    const INDEX &index = indices[size_t(index_type)];

    if (index.buckets > 0) {
        INDEX::TABLE &table = index.table[key.value % index.buckets];

        error = reserve(table.key, capacity);

        if (!error) {
            error = reserve(table.value, capacity);
        }

        return error;
    }

    die();
}

SOCKETS::INDEX::ENTRY SOCKETS::insert(
    INDEX::TYPE index_type, KEY key, PIPE::ENTRY value
) noexcept {
    INDEX &index = indices[size_t(index_type)];

    if (!index.multimap) {
        INDEX::ENTRY found{ find(index_type, key) };

        if (found.valid) {
            return make_index_entry(
                *found.key_pipe, *found.val_pipe, found.index,
                insert(*found.val_pipe, found.index, value)
            );
        }
    }

    if (index.buckets <= 0) die();

    INDEX::TABLE &table = index.table[key.value % index.buckets];
    PIPE &key_pipe = table.key;

    if (key_pipe.type != PIPE::TYPE::KEY) die();

    PIPE &val_pipe = table.value;

    size_t old_size = key_pipe.size;

    ERROR error{insert(key_pipe, make_pipe_entry(key))};

    if (error == ERROR::NONE) {
        error = insert(val_pipe, value);

        if (error == ERROR::NONE) {
            if (++index.entries > index.buckets && index.autogrow) {
                bitset.reindex = true;
            }
        }
        else {
            if (key_pipe.size > old_size) {
                --key_pipe.size;
            }
            else die();
        }
    }

    return make_index_entry(key_pipe, val_pipe, old_size, error);
}

size_t SOCKETS::erase(
    INDEX::TYPE index_type, KEY key, PIPE::ENTRY value,
    size_t start_i, size_t iterations
) noexcept {
    INDEX &index = indices[size_t(index_type)];

    if (index.buckets <= 0) die();

    INDEX::TABLE &table = index.table[key.value % index.buckets];
    PIPE &key_pipe = table.key;

    if (key_pipe.type != PIPE::TYPE::KEY) die();

    KEY *const key_data = to_key(key_pipe);

    size_t erased = 0;

    if (!key_data) {
        return erased;
    }

    PIPE &val_pipe = table.value;

    if (value.type != PIPE::TYPE::NONE && value.type != val_pipe.type) {
        die();
    }

    size_t i{
        // We start from the end because erasing the last element is fast.

        std::min(
            key_pipe.size - 1, start_i
        )
    };

    for (; i < key_pipe.size && iterations; --iterations) {
        if (key_data[i].value != key.value) {
            --i;
            continue;
        }

        if (value.type != PIPE::TYPE::NONE
        && std::memcmp(to_ptr(val_pipe, i), to_ptr(value), size(value.type))) {
            i = index.multimap ? i-1 : key_pipe.size;
            continue;
        }

        erase(key_pipe, i);
        erase(val_pipe, i);

        ++erased;

        if (index.multimap) {
            if (i == key_pipe.size) --i;

            continue;
        }

        break;
    }

    index.entries -= erased;

    return erased;
}

size_t SOCKETS::count(INDEX::TYPE index_type, KEY key) const noexcept {
    size_t count = 0;
    const INDEX &index = indices[size_t(index_type)];

    if (index.buckets > 0) {
        const INDEX::TABLE &table = index.table[key.value % index.buckets];
        const PIPE &pipe = table.key;
        const KEY *const data = to_key(pipe);

        if (data) {
            for (size_t i=0, sz=pipe.size; i<sz; ++i) {
                if (data[i].value == key.value) {
                    ++count;

                    if (!index.multimap) return count;
                }
            }
        }

        return count;
    }

    die();
}

SOCKETS::ERROR SOCKETS::reindex() noexcept {
    for (INDEX &index : indices) {
        if (!index.autogrow || index.entries <= index.buckets) {
            continue;
        }

        const size_t new_buckets = next_pow2(index.entries);

        INDEX::TABLE *new_table = allocate_tables(new_buckets);
        INDEX::TABLE *old_table = index.table;

        if (new_table) {
            for (size_t i=0; i<new_buckets; ++i) {
                new_table[i].key.type = old_table->key.type;
                new_table[i].value.type = old_table->value.type;
            }
        }
        else {
            return ERROR::OUT_OF_MEMORY;
        }

        const size_t old_buckets = index.buckets;
        const size_t old_entries = index.entries;

        index.table = new_table;
        index.buckets = new_buckets;
        index.entries = 0;

        for (size_t i=0; i<old_buckets; ++i) {
            INDEX::TABLE &table = old_table[i];

            for (size_t j=0, sz=table.value.size; j<sz; ++j) {
                INDEX::ENTRY entry{
                    insert(
                        index.type,
                        to_key(get_entry(table.key, j)),
                        get_entry(table.value, j)
                    )
                };

                if (!entry.valid) {
                    index.table = old_table;
                    index.buckets = old_buckets;
                    index.entries = old_entries;

                    destroy_and_delete(new_table, new_buckets);

                    return entry.error;
                }
            }
        }

        destroy_and_delete(old_table, old_buckets);

        if (index.entries != old_entries) {
            report_bug();
        }
    }

    bitset.reindex = false;

    return ERROR::NONE;
}

void SOCKETS::replace(
    PIPE &pipe, size_t index, PIPE::ENTRY value
) const noexcept {
    if (index >= pipe.size) {
        die();
    }
    else if (pipe.type != value.type) {
        die();
    }

    std::memcpy(to_ptr(pipe, index), to_ptr(value), size(value.type));
}

SOCKETS::ERROR SOCKETS::insert(
    PIPE &pipe, size_t index, PIPE::ENTRY value
) noexcept {
    if (index > pipe.size) {
        die();
    }
    else if (pipe.type != value.type) {
        die();
    }
    else if (index == pipe.size) {
        if (pipe.size == pipe.capacity) {
            ERROR error = reserve(pipe, std::max(pipe.size * 2, size_t{1}));

            if (error != ERROR::NONE) {
                return error;
            }
        }

        ++pipe.size;
    }

    replace(pipe, index, value);

    return ERROR::NONE;
}

SOCKETS::ERROR SOCKETS::insert(PIPE &pipe, PIPE::ENTRY value) noexcept {
    return insert(pipe, pipe.size, value);
}

SOCKETS::ERROR SOCKETS::reserve(PIPE &pipe, size_t capacity) noexcept {
    if (pipe.capacity >= capacity) {
        return ERROR::NONE;
    }

    size_t element_size = size(pipe.type);
    size_t byte_count = element_size * capacity;

    if (!byte_count) {
        die();
    }

    MEMORY *const old_memory = pipe.memory;

    if (old_memory && old_memory->size / element_size >= capacity) {
        pipe.capacity = capacity;

        return ERROR::NONE;
    }

    MEMORY *const new_memory = allocate(byte_count, align(pipe.type));

    if (!new_memory) {
        return ERROR::OUT_OF_MEMORY;
    }

    void *const old_data = pipe.data;
    void *const new_data = new_memory->data;

    if (old_data) {
        std::memcpy(new_data, old_data, pipe.size * element_size);
    }

    if (old_memory) {
        recycle(*old_memory);
    }

    pipe.memory = new_memory;
    pipe.data = new_data;
    pipe.capacity = capacity;

    return ERROR::NONE;
}

bool SOCKETS::swap_sessions(JACK *first, JACK *second) noexcept {
    if (!first || !second) return true;

    INDEX::ENTRY first_session_entry{
        find(INDEX::TYPE::SESSION_JACK, make_key(first->id))
    };

    INDEX::ENTRY second_session_entry{
        find(INDEX::TYPE::SESSION_JACK, make_key(second->id))
    };

    if (first_session_entry.valid && second_session_entry.valid) {
        set_value(first_session_entry, make_pipe_entry(second));
        set_value(second_session_entry, make_pipe_entry(first));
        std::swap(first->id, second->id);

        return true;
    }

    report_bug();

    return false;
}

SOCKETS::ERROR SOCKETS::swap(PIPE &first, PIPE &second) noexcept {
    if (first.type == second.type) {
        std::swap(first.capacity, second.capacity);
        std::swap(first.size,     second.size);
        std::swap(first.data,     second.data);
        std::swap(first.memory,   second.memory);

        return ERROR::NONE;
    }

    die();
}

SOCKETS::ERROR SOCKETS::copy(const PIPE &src, PIPE &dst) noexcept {
    if (&src == &dst) {
        return ERROR::NONE;
    }

    if (src.type != dst.type || dst.type == PIPE::TYPE::NONE) {
        die();
    }

    dst.size = 0;

    return append(src, dst);
}

SOCKETS::ERROR SOCKETS::append(const PIPE &src, PIPE &dst) noexcept {
    if (src.type != dst.type || dst.type == PIPE::TYPE::NONE) {
        die();
    }

    size_t old_size = dst.size;
    size_t new_size = old_size + src.size;

    if (new_size > dst.capacity) {
        ERROR error = reserve(dst, new_size);

        if (error != ERROR::NONE) {
            return error;
        }
    }

    size_t count = src.size;

    dst.size = new_size;

    if (src.data == nullptr) {
        return ERROR::NONE;
    }

    std::memcpy(to_ptr(dst, old_size), to_ptr(src, 0), count * size(dst.type));

    return ERROR::NONE;
}

void SOCKETS::erase(PIPE &pipe, size_t index) const noexcept {
    if (index >= pipe.size) {
        die();
    }

    if (index + 1 >= pipe.size) {
        --pipe.size;
        return;
    }

    std::memcpy(
        to_ptr(pipe, index), to_ptr(pipe, pipe.size - 1), size(pipe.type)
    );

    --pipe.size;
}

SOCKETS::PIPE::ENTRY SOCKETS::pop_back(PIPE &pipe) const noexcept {
    size_t size = pipe.size;

    if (size) {
        PIPE::ENTRY entry{get_last(pipe)};

        erase(pipe, size - 1);

        return entry;
    }

    die();
}

SOCKETS::PIPE::ENTRY SOCKETS::get_last(const PIPE &pipe) const noexcept {
    size_t size = pipe.size;

    if (size) {
        return get_entry(pipe, size - 1);
    }

    die();
}

SOCKETS::PIPE::ENTRY SOCKETS::get_entry(
    const PIPE &pipe, size_t index
) const noexcept {
    if (index < pipe.size) {
        PIPE::ENTRY entry{};

        entry.type = pipe.type;

        std::memcpy(to_ptr(entry), to_ptr(pipe, index), size(entry.type));

        return entry;
    }

    die();
}

void SOCKETS::set_value(
    INDEX::ENTRY index_entry, PIPE::ENTRY pipe_entry
) noexcept {
    replace(*index_entry.val_pipe, index_entry.index, pipe_entry);
}

SOCKETS::PIPE::ENTRY SOCKETS::get_value(INDEX::ENTRY entry) const noexcept {
    return get_entry(*entry.val_pipe, entry.index);
}

void SOCKETS::destroy(PIPE &pipe) noexcept {
    if (pipe.memory) {
        recycle(*pipe.memory);
        pipe.memory = nullptr;
    }

    pipe.data = nullptr;
    pipe.capacity = 0;
    pipe.size = 0;
}

void SOCKETS::enlist(MEMORY &memory, MEMORY *&list) noexcept {
    if (memory.next || memory.prev) die();

    memory.next = list;

    if (list) {
        list->prev = &memory;
    }

    list = &memory;
}

void SOCKETS::unlist(MEMORY &memory, MEMORY *&list) noexcept {
    if (memory.indexed) {
        const KEY key{make_key(reinterpret_cast<uintptr_t>(memory.data))};
        INDEX::ENTRY entry{ find(INDEX::TYPE::RESOURCE_MEMORY, key) };

        size_t erased{
            entry.valid ? (
                erase(INDEX::TYPE::RESOURCE_MEMORY, key, {}, entry.index, 1)
            ) : 0
        };

        if (!erased) {
            return die();
        }

        memory.indexed = false;
    }

    if (list == &memory) {
        list = memory.next;

        if (list) {
            list->prev = nullptr;
        }
    }
    else {
        memory.prev->next = memory.next;

        if (memory.next) {
            memory.next->prev = memory.prev;
        }
    }

    memory.next = nullptr;
    memory.prev = nullptr;
}

const SOCKETS::MEMORY *SOCKETS::find_memory(
    const void *resource
) const noexcept {
    INDEX::ENTRY entry{
        find(
            INDEX::TYPE::RESOURCE_MEMORY,
            make_key(reinterpret_cast<uintptr_t>(resource))
        )
    };

    if (entry.valid) {
        return to_memory(get_value(entry));
    }

    die();
}

SOCKETS::MEMORY *SOCKETS::find_memory(const void *resource) noexcept {
    return const_cast<MEMORY *>(
        static_cast<const SOCKETS &>(*this).find_memory(resource)
    );
}

const SOCKETS::MEMORY &SOCKETS::get_memory(
    const void *resource
) const noexcept {
    const MEMORY *const memory = find_memory(resource);

    if (memory) {
        return *memory;
    }

    die();
}

SOCKETS::MEMORY &SOCKETS::get_memory(const void *resource) noexcept {
    MEMORY *const memory = find_memory(resource);

    if (memory) {
        return *memory;
    }

    die();
}

SOCKETS::INDEX::TABLE *SOCKETS::allocate_tables(size_t count) noexcept {
    const size_t total_size = sizeof(INDEX::TABLE) * count;
    const auto usage_left{
        std::numeric_limits<decltype(mempool.usage)>::max() - mempool.usage
    };

    INDEX::TABLE *tables = (
        usage_left >= total_size &&
        mempool.cap >= mempool.usage + total_size ? (
            new (std::nothrow) INDEX::TABLE [count]()
        ) : nullptr
    );

    if (!tables) {
        return nullptr;
    }

    mempool.usage += total_size;

    return tables;
}

void SOCKETS::destroy_and_delete(INDEX::TABLE *tables, size_t count) noexcept {
    for (size_t i=0; i<count; ++i) {
        destroy(tables[i].key);
        destroy(tables[i].value);
    }

    delete [] tables;

    mempool.usage -= sizeof(INDEX::TABLE) * count;
}

SOCKETS::MEMORY *SOCKETS::allocate(
    const size_t requested_byte_count, size_t align
) noexcept {
    align = std::max(alignof(MEMORY), align);
    size_t byte_count = next_pow2(requested_byte_count); // Always at least 1.
    const size_t padding = (align - sizeof(MEMORY) % align) % align;
    const size_t total_size = sizeof(MEMORY) + padding + byte_count;
    MEMORY *memory = nullptr;

    do {
        MEMORY *&free = mempool.free[clz(byte_count)];

        if (!free) {
            break;
        }

        for (MEMORY *m = free; m != nullptr; m = m->next) {
            const size_t old_padding{
                reinterpret_cast<uintptr_t>(m->data) -
                reinterpret_cast<uintptr_t>(m) - sizeof(MEMORY)
            };

            if (padding > old_padding) {
                const size_t deficit = padding - old_padding;

                if (m->size < requested_byte_count + deficit) {
                    continue;
                }

                m->size -= deficit;
            }
            else {
                m->size += old_padding - padding;
            }

            m->data = reinterpret_cast<void *>(
                reinterpret_cast<uintptr_t>(m) + sizeof(MEMORY) + padding
            );

            unlist(*m, free);
            memory = m;

            break;
        }
    }
    while (false);

    if (!memory) {
        const auto usage_left{
            std::numeric_limits<decltype(mempool.usage)>::max() - mempool.usage
        };

        for (size_t i=0; i<2; ++i) {
            memory = static_cast<MEMORY *>(
                usage_left >= total_size &&
                mempool.cap >= mempool.usage + total_size ? (
                    std::aligned_alloc(align, total_size)
                ) : nullptr
            );

            if (!memory) {
                for (MEMORY *&free : mempool.free) {
                    while (free) {
                        deallocate(*free);
                    }
                }
            }
        }

        if (!memory) {
            mempool.oom = true;
            return nullptr;
        }

        if (reinterpret_cast<std::uintptr_t>(memory) % alignof(MEMORY)) {
            if (fuse()) report_bug("misaligned pointer detected");
        }

        mempool.usage += total_size;

        memory->size = byte_count;
        memory->data = reinterpret_cast<void *>(
            reinterpret_cast<uintptr_t>(memory) + sizeof(MEMORY) + padding
        );
    }

    memory->next = nullptr;
    memory->prev = nullptr;
    memory->indexed = false;
    memory->recycled = false;

    enlist(*memory, mempool.list);

    if (reinterpret_cast<std::uintptr_t>(memory->data) % align) {
        if (fuse()) report_bug("misaligned pointer detected");
    }

    return memory;
}

SOCKETS::MEMORY *SOCKETS::allocate_and_index(
    size_t byte_count, size_t alignment, const void *copy
) noexcept {
    MEMORY *memory = allocate(byte_count, alignment);

    if (!memory) {
        return nullptr;
    }

    if (copy) {
        std::memcpy(memory->data, copy, memory->size);
    }

    INDEX::ENTRY entry{
        insert(
            INDEX::TYPE::RESOURCE_MEMORY,
            make_key(reinterpret_cast<uintptr_t>(memory->data)),
            make_pipe_entry(memory)
        )
    };

    if (entry.valid) {
        memory->indexed = true;
    }
    else {
        recycle(*memory);
        return nullptr;
    }

    return memory;
}

void SOCKETS::deallocate(MEMORY &memory) noexcept {
    if (memory.recycled) {
        MEMORY *&free = mempool.free[clz(memory.size)];
        unlist(memory, free);
    }
    else {
        unlist(memory, mempool.list);
    }

    const size_t total_size{
        reinterpret_cast<uintptr_t>(memory.data) -
        reinterpret_cast<uintptr_t>(&memory) + memory.size
    };

    std::free(&memory);

    if (total_size <= mempool.usage) {
        mempool.usage -= total_size;
    }
    else {
        if (fuse()) report_bug("detected memory usage tracking corruption");

        mempool.usage = 0;
    }
}

void SOCKETS::recycle(MEMORY &memory) noexcept {
    if (memory.recycled) {
        return;
    }

    unlist(memory, mempool.list);
    enlist(memory, mempool.free[clz(memory.size)]);

    memory.recycled = true;
}

SOCKETS::JACK *SOCKETS::new_jack(const JACK *copy) noexcept {
    MEMORY *const mem = allocate_and_index(sizeof(JACK), alignof(JACK), copy);

    return mem ? reinterpret_cast<JACK *>(mem->data) : nullptr;
}

SOCKETS::RESULT SOCKETS::call_sigfillset(
    sigset_t *set, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = sigfillset(set), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EINVAL: //____________________________________ Invalid argument
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_sigemptyset(
    sigset_t *set, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = sigemptyset(set), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EINVAL: //____________________________________ Invalid argument
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_epoll_create1(
    int flags, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = epoll_create1(flags), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EINVAL: //____________________________________ Invalid argument
            {
                error = ERROR::LIBRARY;
                break;
            }
            case ENOMEM: //_____________ Not enough space/cannot allocate memory
            {
                error = ERROR::OUT_OF_MEMORY;
                break;
            }
            case EMFILE: // ________________________________ Too many open files
            case ENFILE: //_______________________ Too many open files in system
            {
                error = ERROR::RES_LIMIT_MET;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (!is_descriptor(retval)) {
            str = "unexpected return value";
            error = ERROR::UNKNOWN;
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_epoll_pwait(
    int epfd, struct epoll_event *events, int maxevents,
    int timeout, const sigset_t *sigmask, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = epoll_pwait(
        epfd, events, maxevents, timeout, sigmask
    ), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EINTR: //____________________________ Interrupted function call
            {
                break;
            }
            case EBADF: //__________________________________ Bad file descriptor
            case EFAULT: //_________________________________________ Bad address
            case EINVAL: //____________________________________ Invalid argument
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval < 0) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_getsockopt(
    int sockfd, int level, int optname, void *optval, socklen_t *optlen,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = getsockopt(
        sockfd, level, optname, optval, optlen
    ), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case ENOPROTOOPT: //_________________________ Protocol not available
            case ENOTSOCK:  //_____________________________________ Not a socket
            case EFAULT: //_________________________________________ Bad address
            case EINVAL: //____________________________________ Invalid argument
            case EBADF:  //_________________________________ Bad file descriptor
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_accept4(
    int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = accept4(sockfd, addr, addrlen, flags), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
#if EAGAIN != EWOULDBLOCK
            case EAGAIN: //____________________ Resource temporarily unavailable
#endif
            case EWOULDBLOCK: //__________________________ Operation would block
#if ENOTSUP != EOPNOTSUPP
            case ENOTSUP: //____________________________ Operation not supported
#endif
            case EOPNOTSUPP: //_______________ Operation not supported on socket
            case EPROTO: //______________________________________ Protocol error
            case ENOPROTOOPT: //_________________________ Protocol not available
            case ENETDOWN: //___________________________________ Network is down
            case EHOSTDOWN: //_____________________________________ Host is down
            case ENONET: //_______________________ Machine is not on the network
            case EHOSTUNREACH: //___________________________ Host is unreachable
            case ENETUNREACH: //____________________________ Network unreachable
            case ECONNABORTED: //____________________________ Connection aborted
            case EPERM: //_____________________ Firewall rules forbid connection
            case ETIMEDOUT: //_____________________________ Connection timed out
            case ESOCKTNOSUPPORT: //__________________ Socket type not supported
            case EPROTONOSUPPORT: //_____________________ Protocol not supported
            case EINTR: //____________________________ Interrupted function call
            {
                break;
            }
            case ENOTSOCK: //______________________________________ Not a socket
            case EINVAL: //____________________________________ Invalid argument
            case EFAULT: //_________________________________________ Bad address
            case EBADF: //__________________________________ Bad file descriptor
            {
                error = ERROR::LIBRARY;
                break;
            }
            case ENOSR: //__________________________________ No STREAM resources
            case ENOBUFS: //__________________________ No buffer space available
            case ENOMEM: //_____________ Not enough space/cannot allocate memory
            case ENFILE: //_______________________ Too many open files in system
            case EMFILE: //_________________________________ Too many open files
            {
                error = ERROR::RES_LIMIT_MET;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (!is_descriptor(retval)) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_getnameinfo(
    const struct sockaddr *addr, socklen_t addrlen, char *host,
    socklen_t hostlen, char *serv, socklen_t servlen, int flags,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = getnameinfo(
        addr, addrlen, host, hostlen, serv, servlen, flags
    ), code = errno;

    if (retval != 0) {
        int codebuf = code;
        str = gai_strerror(retval);
        code= retval;

        switch (retval) {
            case EAI_AGAIN: //__ Name could not be resolved yet, try again later
            {
                error = ERROR::BAD_TIMING;
                break;
            }
            case EAI_NONAME: // Name does not resolve for the supplied arguments
            case EAI_FAIL: //___________________ A nonrecoverable error occurred
            {
                error = ERROR::SYSTEM;
                break;
            }
            case EAI_OVERFLOW: //___ Buffer pointed to by host or serv too small
            case EAI_BADFLAGS: //_______ The flags argument has an invalid value
            case EAI_FAMILY: //_ Family not recognized or address length invalid
            {
                error = ERROR::LIBRARY;
                break;
            }
            case EAI_MEMORY: //___________________________________ Out of memory
            {
                error = ERROR::OUT_OF_MEMORY;
                break;
            }
            case EAI_SYSTEM: //___ System error occurred, error code is in errno
            {
                code = codebuf;
                str = strerror(code);
                error = ERROR::SYSTEM;
                break;
            }
            default: {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else code = 0;

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_shutdown(
    int sockfd, int how, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = shutdown(sockfd, how), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case ENOTSOCK: //______________________________________ Not a socket
            case ENOTCONN: //_______________________ The socket is not connected
            case EBADF: //__________________________________ Bad file descriptor
            case EINVAL: //____________________________________ Invalid argument
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_listen(
    int sockfd, int backlog, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = ::listen(sockfd, backlog), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
#if ENOTSUP != EOPNOTSUPP
            case ENOTSUP: //____________________________ Operation not supported
#endif
            case EOPNOTSUPP: //_______________ Operation not supported on socket
            case EBADF: //__________________________________ Bad file descriptor
            case ENOTSOCK: //______________________________________ Not a socket
            {
                error = ERROR::LIBRARY;
                break;
            }
            case EADDRINUSE: //__________________________ Address already in use
            {
                error = ERROR::SYSTEM;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_getsockname(
    int sockfd, struct sockaddr *addr, socklen_t *addrlen,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = getsockname(sockfd, addr, addrlen), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EBADF:
            case EFAULT:
            case EINVAL:
            case ENOTSOCK:
            {
                error = ERROR::LIBRARY;
                break;
            }
            case ENOBUFS:
            {
                error = ERROR::RES_LIMIT_MET;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_epoll_ctl(
    int epfd, int op, int fd, struct epoll_event *event,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = epoll_ctl(epfd, op, fd, event), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EPERM: //______________________________ Operation not permitted
            case ENOENT: //___________________________ No such file or directory
            case ELOOP: //____________________ Too many levels of symbolic links
            case EINVAL: //____________________________________ Invalid argument
            case EEXIST: //_________________________________________ File exists
            case EBADF: //__________________________________ Bad file descriptor
            {
                error = ERROR::LIBRARY;
                break;
            }
            case ENOSPC: //_____________________________ No space left on device
            {
                error = ERROR::RES_LIMIT_MET;
                break;
            }
            case ENOMEM: //_____________ Not enough space/cannot allocate memory
            {
                error = ERROR::OUT_OF_MEMORY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_pthread_sigmask(
    int how, const sigset_t *set, sigset_t *oldset, const char *file, int line
) const noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = pthread_sigmask(how, set, oldset), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EFAULT: //_________________________________________ Bad address
            case EINVAL: //____________________________________ Invalid argument
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_getaddrinfo(
    const char *node, const char *service, const struct addrinfo *hints,
    struct addrinfo **res, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = getaddrinfo(node, service, hints, res), code = errno;

    if (retval != 0) {
        int codebuf = code;
        str = gai_strerror(retval);
        code= retval;

        switch (retval) {
            case EAI_AGAIN: //__ Name could not be resolved yet, try again later
            {
                error = ERROR::BAD_TIMING;
                break;
            }
            case EAI_FAIL: //___________________ A nonrecoverable error occurred
            {
                error = ERROR::SYSTEM;
                break;
            }
            case EAI_ADDRFAMILY: //___ Host has no addresses in requested family
            case EAI_NODATA: //________________ The node or service is not known
            case EAI_NONAME: //________ The node or service is not known; or ...
            case EAI_SERVICE: //______ service not available for the socket type
            {
                error = ERROR::SYSTEM;
                break;
            }
            case EAI_FAMILY: //_ Family not recognized or address length invalid
            case EAI_BADFLAGS: //____________ Hints contain invalid flags or ...
            case EAI_SOCKTYPE: //________ Requested socket type is not supported
            {
                error = ERROR::LIBRARY;
                break;
            }
            case EAI_MEMORY: //___________________________________ Out of memory
            {
                error = ERROR::OUT_OF_MEMORY;
                break;
            }
            case EAI_SYSTEM: //___ System error occurred, error code is in errno
            {
                code = codebuf;
                str = strerror(code);
                error = ERROR::SYSTEM;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else code = 0;

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_socket(
    int domain, int type, int protocol, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = socket(domain, type, protocol), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EACCES: //______ Socket of the given type or protocol is denied
            {
                error = ERROR::SYSTEM;
                break;
            }
            case EPROTONOSUPPORT: //__ Protocol not supported within this domain
            case EAFNOSUPPORT: //________ Specified address family not supported
            {
                error = ERROR::SYSTEM;
                break;
            }
            case EINVAL: //__ Unknown protocol, or protocol family not available
            {
                error = ERROR::LIBRARY;
                break;
            }
            case ENFILE: //________  Total number of open files has been reached
            case EMFILE: //_________ Open file descriptor limit has been reached
            {
                error = ERROR::RES_LIMIT_MET;
                break;
            }
            case ENOBUFS: //___________________ Insufficient memory is available
            case ENOMEM: //____________________ Insufficient memory is available
            {
                error = ERROR::OUT_OF_MEMORY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (!is_descriptor(retval)) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_setsockopt(
    int sockfd, int level, int optname, const void *optval, socklen_t optlen,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = setsockopt(
        sockfd, level, optname, optval, optlen
    ), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EBADF: //_______ Argument sockfd is not a valid file descriptor
            case EFAULT: //_____ Argument optval is in an invalid part of memory
            case EINVAL: //________________ Argument optlen or optval is invalid
            case ENOPROTOOPT: //___ The option is unknown at the level indicated
            case ENOTSOCK: //________ Argument sockfd does not refer to a socket
            {
                error = ERROR::LIBRARY;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_bind(
    int sockfd, const struct sockaddr *addr, socklen_t addrlen,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = bind(sockfd, addr, addrlen), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case ENOMEM: //____________ Insufficient kernel memory was available
            {
                error = ERROR::OUT_OF_MEMORY;
                break;
            }
            case EFAULT: //_ Address points outside the user's accessible memory
            case ENOTSOCK: //______ Descriptor sockfd does not refer to a socket
            case EINVAL: //___________ The socket is already bound to an address
            case EBADF: //____________ Descriptor is not a valid file descriptor
            {
                error = ERROR::LIBRARY;
                break;
            }
            case EROFS: //__ Socket inode would reside on a read-only filesystem
            case ENOTDIR: //__ A component of the path prefix is not a directory
            case ENOENT: //_____ component in the socket pathname does not exist
            case ELOOP: //______ Too many symbolic links while resolving address
            case EADDRNOTAVAIL: //__________ Nonexistent interface was requested
            case EACCES: //__ Address is protected and user is not the superuser
            case ENAMETOOLONG: //___________________________ Address is too long
            case EADDRINUSE: //_____________ The given address is already in use
            {
                error = ERROR::SYSTEM;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_connect(
    int sockfd, const struct sockaddr *addr, socklen_t addrlen,
    const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = ::connect(sockfd, addr, addrlen), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EINTR: //______________ System call was interrupted by a signal
            case EAGAIN: //__________ Connection cannot be completed immediately
            case EINPROGRESS: //_____ Connection cannot be completed immediately
            case ENETUNREACH: //_________________________ Network is unreachable
            case ECONNREFUSED: //________ No one listening on the remote address
            {
                break;
            }
            case EADDRNOTAVAIL: //__ All ports in the ephemeral range are in use
            case EPERM: //______ Request failed because of a local firewall rule
            case EACCES: //____________ Write permission is denied on the socket
            case EADDRINUSE: //_________________ Local address is already in use
            {
                error = ERROR::SYSTEM;
                break;
            }
            case EPROTOTYPE: //__ Socket does not support the requested protocol
            case ENOTSOCK: //_____________ Descriptor does not refer to a socket
            case EISCONN: //________________________ Socket is already connected
            case EFAULT: //__ Socket address is outside the user's address space
            case EBADF: //_______ Descriptor is not a valid open file descriptor
            case EALREADY: //_______ Previous attempt has not yet been completed
            case EAFNOSUPPORT: //__________ Address has incorrect address family
            {
                error = ERROR::LIBRARY;
                break;
            }
            case ETIMEDOUT: //______________ Timeout while attempting connection
            {
                error = ERROR::BAD_TIMING;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

SOCKETS::RESULT SOCKETS::call_close(
    int fd, const char *file, int line
) noexcept {
    int errno_orig = errno;
    errno = 0;

    ERROR error = ERROR::NONE;
    const char *str = "";
    int retval = close(fd), code = errno;

    if (retval == -1) {
        str = strerror(code);

        switch (code) {
            case EINTR: //_____________________ Call was interrupted by a signal
            {
                break;
            }
            case EBADF: //_______ Descriptor is not a valid open file descriptor
            {
                error = ERROR::LIBRARY;
                break;
            }
            case EIO: //__________________________________ An I/O error occurred
            {
                error = ERROR::SYSTEM;
                break;
            }
            case ENOSPC: //_____________________________ No space left on device
            case EDQUOT: //_________________________________ Disk quota exceeded
            {
                error = ERROR::RES_LIMIT_MET;
                break;
            }
            default:
            {
                error = ERROR::SYSTEM;
                break;
            }
        }
    }
    else {
        code = 0;

        if (retval) {
            error = ERROR::UNKNOWN;
            str = "unexpected return value";
        }
    }

    errno = errno_orig;

    return make_result(
        retval, code, error, str, TAIL(__FUNCTION__, '_'), file, line
    );
}

constexpr SOCKETS::RESULT SOCKETS::make_result(
    int value, int code, ERROR error,
    const char *text, const char *call, const char *file, int line
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    RESULT{
        .value = value,
        .code  = code,
        .text  = text,
        .call  = call,
        .file  = file,
        .line  = line,
        .error = error
    };
}

constexpr SOCKETS::JACK SOCKETS::make_jack(
    int descriptor, int parent
) noexcept {
#if __cplusplus <= 201703L
    __extension__
#endif
    JACK jack{
        .id           = 0,
        .intake       = std::numeric_limits<size_t>::max(),
        .event_lookup = {},
        .epoll_ev     = { make_pipe(PIPE::TYPE::EPOLL_EVENT) },
        .children     = { make_pipe(PIPE::TYPE::INT  ) },
        .incoming     = { make_pipe(PIPE::TYPE::UINT8) },
        .outgoing     = { make_pipe(PIPE::TYPE::UINT8) },
        .host         = { make_pipe(PIPE::TYPE::UINT8) },
        .port         = { make_pipe(PIPE::TYPE::UINT8) },
        .descriptor   = descriptor,
        .parent       = { .descriptor = parent, .child_index = -1 },
        .ai_family    = 0,
        .ai_flags     = 0,
        .blacklist    = nullptr,
        .bitset       = {}
    };

    for (auto &lookup_value : jack.event_lookup) {
        lookup_value = std::numeric_limits<unsigned>::max();
    }

    return jack;
}

constexpr SOCKETS::ALERT SOCKETS::make_alert(
    size_t session, EVENT event, bool valid
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    ALERT{
        .session = session,
        .event = event,
        .valid = valid
    };
}

constexpr SOCKETS::SESSION SOCKETS::make_session(
    size_t id, ERROR error, bool valid
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SESSION{
        .id    = id,
        .error = error,
        .valid = valid
    };
}

constexpr SOCKETS::SESSION SOCKETS::make_session(ERROR error) noexcept {
    return make_session(0, error, false);
}

constexpr struct SOCKETS::INDEX::ENTRY SOCKETS::make_index_entry(
    PIPE &keys, PIPE &values, size_t index, ERROR error, bool valid
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::INDEX::ENTRY{
        .key_pipe = &keys,
        .val_pipe = &values,
        .index    = index,
        .error    = error,
        .valid    = valid
    };
}

constexpr struct SOCKETS::INDEX::ENTRY SOCKETS::make_index_entry(
    PIPE &keys, PIPE &values, size_t index, ERROR error
) noexcept {
    return make_index_entry(keys, values, index, error, error == ERROR::NONE);
}

constexpr SOCKETS::PIPE SOCKETS::make_pipe(
    const uint8_t *data, size_t size
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE{
        .capacity = size,
        .size = size,
        .data = const_cast<uint8_t *>(data),
        .type = PIPE::TYPE::UINT8,
        .memory = nullptr
    };
}

constexpr SOCKETS::PIPE SOCKETS::make_pipe(PIPE::TYPE type) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE{
        .capacity = 0,
        .size = 0,
        .data = nullptr,
        .type = type,
        .memory = nullptr
    };
}

constexpr struct SOCKETS::PIPE::ENTRY SOCKETS::make_pipe_entry(
    PIPE::TYPE type
) noexcept {
    PIPE::ENTRY entry{};
    entry.type = type;
    return entry;
}

constexpr struct SOCKETS::PIPE::ENTRY SOCKETS::make_pipe_entry(
    uint64_t value
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE::ENTRY{
        .as_uint64 = value,
        .type = PIPE::TYPE::UINT64
    };
}

constexpr struct SOCKETS::PIPE::ENTRY SOCKETS::make_pipe_entry(
    int value
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE::ENTRY{
        .as_int = value,
        .type = PIPE::TYPE::INT
    };
}

constexpr struct SOCKETS::PIPE::ENTRY SOCKETS::make_pipe_entry(
    KEY value
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE::ENTRY{
        .as_key = value,
        .type = PIPE::TYPE::KEY
    };
}

constexpr struct SOCKETS::PIPE::ENTRY SOCKETS::make_pipe_entry(
    JACK *value
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE::ENTRY{
        .as_ptr = value,
        .type = PIPE::TYPE::JACK_PTR
    };
}

constexpr struct SOCKETS::PIPE::ENTRY SOCKETS::make_pipe_entry(
    MEMORY *value
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::PIPE::ENTRY{
        .as_ptr = value,
        .type = PIPE::TYPE::MEMORY_PTR
    };
}

constexpr SOCKETS::MEMPOOL SOCKETS::make_mempool() noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::MEMPOOL{
        .free  = {},
        .list  = nullptr,
        .usage = 0,
        .top   = 0,
        .cap   = std::numeric_limits<decltype(MEMPOOL::cap)>::max(),
        .oom   = false
    };
}

constexpr SOCKETS::KEY SOCKETS::make_key(uintptr_t val) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    SOCKETS::KEY{
        .value = val
    };
}

constexpr SOCKETS::KEY SOCKETS::make_key(EVENT val) noexcept {
    return make_key(static_cast<uintptr_t>(val));
}

constexpr epoll_data_t SOCKETS::make_epoll_data(int descriptor) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    epoll_data_t{
        .fd = descriptor
    };
}

constexpr struct epoll_event SOCKETS::make_epoll_event(
    int descriptor, uint32_t events
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    epoll_event{
        .events = events,
        .data   = make_epoll_data(descriptor)
    };
}

constexpr struct addrinfo SOCKETS::make_addrinfo(
    int ai_family, int ai_flags
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    addrinfo{
        .ai_flags     = ai_flags,
        .ai_family    = ai_family,
        .ai_socktype  = SOCK_STREAM,
        .ai_protocol  = 0,
        .ai_addrlen   = 0,
        .ai_addr      = nullptr,
        .ai_canonname = nullptr,
        .ai_next      = nullptr
    };
}

constexpr struct SOCKETS::QUERY SOCKETS::make_descriptor_query(
    int descriptor
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    QUERY{
        .descriptor = descriptor,
        .type       = QUERY::TYPE::DESCRIPTOR
    };
}

constexpr struct SOCKETS::QUERY SOCKETS::make_session_query(
    size_t session
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    QUERY{
        .session = session,
        .type    = QUERY::TYPE::SESSION
    };
}

constexpr struct SOCKETS::QUERY SOCKETS::make_session_query(
    SESSION session
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    QUERY{
        .session = session.id,
        .type    = QUERY::TYPE::SESSION
    };
}

constexpr struct SOCKETS::QUERY SOCKETS::make_event_query(
    EVENT event
) noexcept {
    return
#if __cplusplus <= 201703L
    __extension__
#endif
    QUERY{
        .event = event,
        .type  = QUERY::TYPE::EVENT
    };
}

bool SOCKETS::is_listed(const addrinfo &info, const addrinfo *list) noexcept {
    for (const struct addrinfo *next = list; next; next = next->ai_next) {
        if (info.ai_flags != next->ai_flags
        ||  info.ai_family != next->ai_family
        ||  info.ai_socktype != next->ai_socktype
        ||  info.ai_protocol != next->ai_protocol
        ||  info.ai_addrlen != next->ai_addrlen) {
            continue;
        }

        if ((info.ai_canonname && !next->ai_canonname)
        ||  (next->ai_canonname && !info.ai_canonname)) {
            continue;
        }

        if (info.ai_canonname && next->ai_canonname
        && std::strcmp(info.ai_canonname, next->ai_canonname)) {
            continue;
        }

        const void *const first = static_cast<const void *>(info.ai_addr);
        const void *const second = static_cast<const void *>(next->ai_addr);

        if (std::memcmp(first, second, (size_t) info.ai_addrlen) != 0) {
            continue;
        }

        return true;
    }

    return false;
}

constexpr bool SOCKETS::is_descriptor(int d) noexcept {
    return d >= 0;
}

const char *SOCKETS::to_string(ERROR error) noexcept {
    switch (error) {
        case ERROR::NONE:          return "no error";
        case ERROR::BAD_TIMING:    return "bad timing";
        case ERROR::PENDING_ALERT: return "unhandled events";
        case ERROR::OUT_OF_MEMORY: return "out of memory";
        case ERROR::RES_LIMIT_MET: return "resource limit met";
        case ERROR::LIBRARY:       return "library error";
        case ERROR::BAD_REQUEST:   return "invalid request";
        case ERROR::SYSTEM:        return "system error";
        case ERROR::UNKNOWN:       return "unknown error";
    }

    return "unknown error";
}

constexpr SOCKETS::EVENT SOCKETS::next(EVENT event_type) noexcept {
    return static_cast<EVENT>(
        (static_cast<size_t>(event_type) + 1) % (
            static_cast<size_t>(EVENT::MAX_EVENTS)
        )
    );
}

constexpr size_t SOCKETS::size(PIPE::TYPE type) noexcept {
    switch (type) {
        case PIPE::TYPE::UINT8:       return sizeof(uint8_t);
        case PIPE::TYPE::UINT64:      return sizeof(uint64_t);
        case PIPE::TYPE::INT:         return sizeof(int);
        case PIPE::TYPE::PTR:         return sizeof(void *);
        case PIPE::TYPE::JACK_PTR:    return sizeof(JACK *);
        case PIPE::TYPE::MEMORY_PTR:  return sizeof(MEMORY *);
        case PIPE::TYPE::KEY:         return sizeof(KEY);
        case PIPE::TYPE::EPOLL_EVENT: return sizeof(epoll_event);
        case PIPE::TYPE::NONE:        break;
    }

    return 0;
}

constexpr size_t SOCKETS::align(PIPE::TYPE type) noexcept {
    switch (type) {
        case PIPE::TYPE::UINT8:       return alignof(uint8_t);
        case PIPE::TYPE::UINT64:      return alignof(uint64_t);
        case PIPE::TYPE::INT:         return alignof(int);
        case PIPE::TYPE::PTR:         return alignof(void *);
        case PIPE::TYPE::JACK_PTR:    return alignof(JACK *);
        case PIPE::TYPE::MEMORY_PTR:  return alignof(MEMORY *);
        case PIPE::TYPE::KEY:         return alignof(KEY);
        case PIPE::TYPE::EPOLL_EVENT: return alignof(epoll_event);
        case PIPE::TYPE::NONE:        break;
    }

    return 0;
}

constexpr const char *SOCKETS::LEAF(const char* path) noexcept {
    const char* file = path;

    while (*path) {
        if (*path++ == '/') {
            file = path;
        }
    }

    return file;
}

constexpr const char *SOCKETS::TAIL(const char* snake, char neck) noexcept {
    const char* tail = snake;

    while (*snake) {
        if (*snake++ == neck) {
            tail = snake;
            break;
        }
    }

    return tail;
}

int SOCKETS::clz(unsigned int x) noexcept {
    return __builtin_clz(x);
}

int SOCKETS::clz(unsigned long x) noexcept {
    return __builtin_clzl(x);
}

int SOCKETS::clz(unsigned long long x) noexcept {
    return __builtin_clzll(x);
}

unsigned int SOCKETS::next_pow2(unsigned int x) noexcept {
    return x <= 1 ? 1 : 1 << ((sizeof(x) * BITS_PER_BYTE) - clz(x - 1));
}

unsigned long SOCKETS::next_pow2(unsigned long x) noexcept {
    return x <= 1 ? 1 : 1 << ((sizeof(x) * BITS_PER_BYTE) - clz(x - 1));
}

unsigned long long SOCKETS::next_pow2(unsigned long long x) noexcept {
    return x <= 1 ? 1 : 1 << ((sizeof(x) * BITS_PER_BYTE) - clz(x - 1));
}

static_assert(
    __LINE__ < sizeof(SOCKETS::fuses) * SOCKETS::BITS_PER_BYTE,
    "number of fuse bits should exceed the line count of this file"
);

#endif
