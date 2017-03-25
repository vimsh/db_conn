/*
 * File:   sqlite_driver.hpp
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#ifndef SQLITE_DRIVER_HPP
#define SQLITE_DRIVER_HPP

#include <climits>
#include <cstring>
#include <sqlite3.h>
#include <algorithm>
#include <vector>
#include "driver.hpp"

namespace vgi { namespace dbconn { namespace dbd { namespace sqlite {

enum class open_flag : int
{
    READONLY        = SQLITE_OPEN_READONLY,
    READWRITE       = SQLITE_OPEN_READWRITE,
    CREATE          = SQLITE_OPEN_CREATE,
    DELETEONCLOSE   = SQLITE_OPEN_DELETEONCLOSE,
    EXCLUSIVE       = SQLITE_OPEN_EXCLUSIVE,
#ifdef SQLITE_OPEN_AUTOPROXY
    AUTOPROXY       = SQLITE_OPEN_AUTOPROXY,
#endif
#ifdef SQLITE_OPEN_URI
    URI             = SQLITE_OPEN_URI,
#endif
#ifdef SQLITE_OPEN_MEMORY
    MEMORY          = SQLITE_OPEN_MEMORY,
#endif
    MAIN_DB         = SQLITE_OPEN_MAIN_DB,
    TEMP_DB         = SQLITE_OPEN_TEMP_DB,
    TRANSIENT_DB    = SQLITE_OPEN_TRANSIENT_DB,
    MAIN_JOURNAL    = SQLITE_OPEN_MAIN_JOURNAL,
    TEMP_JOURNAL    = SQLITE_OPEN_TEMP_JOURNAL,
    SUBJOURNAL      = SQLITE_OPEN_SUBJOURNAL,
    MASTER_JOURNAL  = SQLITE_OPEN_MASTER_JOURNAL,
    NOMUTEX         = SQLITE_OPEN_NOMUTEX,
    FULLMUTEX       = SQLITE_OPEN_FULLMUTEX,
    SHAREDCACHE     = SQLITE_OPEN_SHAREDCACHE,
    PRIVATECACHE    = SQLITE_OPEN_PRIVATECACHE,
#ifdef SQLITE_OPEN_WAL
    WAL             = SQLITE_OPEN_WAL
#endif
};

constexpr open_flag operator|(open_flag l, open_flag r) { return open_flag(utils::base_type(l) | utils::base_type(r)); }


enum class config_flag : int
{
#ifdef SQLITE_CONFIG_LOG
    LOG             = SQLITE_CONFIG_LOG,
#endif
#ifdef SQLITE_CONFIG_URI
    URI             = SQLITE_CONFIG_URI,
#endif
#ifdef SQLITE_CONFIG_PCACHE2
    PCACHE2         = SQLITE_CONFIG_PCACHE2,
#endif
#ifdef SQLITE_CONFIG_GETPCACHE2
    GETPCACHE2      = SQLITE_CONFIG_GETPCACHE2,
#endif
#ifdef SQLITE_CONFIG_COVERING_INDEX_SCAN
    COVERING_INDEX  = SQLITE_CONFIG_COVERING_INDEX_SCAN,
#endif
#ifdef SQLITE_CONFIG_SQLLOG
    SQLLOG          = SQLITE_CONFIG_SQLLOG,
#endif
#ifdef SQLITE_CONFIG_MMAP_SIZE
    MMAP_SIZE       = SQLITE_CONFIG_MMAP_SIZE,
#endif
#ifdef SQLITE_CONFIG_WIN32_HEAPSIZE
    WIN32_HEAPSIZE  = SQLITE_CONFIG_WIN32_HEAPSIZE,
#endif
#ifdef SQLITE_CONFIG_PCACHE_HDRSZ
    PCACHE_HDRSZ    = SQLITE_CONFIG_PCACHE_HDRSZ,
#endif
#ifdef SQLITE_CONFIG_PMASZ
    PMASZ           = SQLITE_CONFIG_PMASZ,
#endif
#ifdef SQLITE_CONFIG_STMTJRNL_SPILL
    STMTJRNL_SPILL  = SQLITE_CONFIG_STMTJRNL_SPILL,
#endif
    SINGLETHREAD    = SQLITE_CONFIG_SINGLETHREAD,
    MULTITHREAD     = SQLITE_CONFIG_MULTITHREAD,
    SERIALIZED      = SQLITE_CONFIG_SERIALIZED,
    MALLOC          = SQLITE_CONFIG_MALLOC,
    GETMALLOC       = SQLITE_CONFIG_GETMALLOC,
    SCRATCH         = SQLITE_CONFIG_SCRATCH,
    PAGECACHE       = SQLITE_CONFIG_PAGECACHE,
    HEAP            = SQLITE_CONFIG_HEAP,
    MEMSTATUS       = SQLITE_CONFIG_MEMSTATUS,
    MUTEX           = SQLITE_CONFIG_MUTEX,
    GETMUTEX        = SQLITE_CONFIG_GETMUTEX,
    LOOKASIDE       = SQLITE_CONFIG_LOOKASIDE,
    PCACHE          = SQLITE_CONFIG_PCACHE,
    GETPCACHE       = SQLITE_CONFIG_GETPCACHE,
    // extra flag for functions other than sqlite3_config
    SOFT_HEAP_LIMIT,
};

enum class db_config_flag : int
{
#ifdef SQLITE_DBCONFIG_MAINDBNAME
    MAINDBNAME            = SQLITE_DBCONFIG_MAINDBNAME,
#endif
#ifdef SQLITE_DBCONFIG_ENABLE_FKEY
    ENABLE_FKEY           = SQLITE_DBCONFIG_ENABLE_FKEY,
#endif
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
    ENABLE_TRIGGER        = SQLITE_DBCONFIG_ENABLE_TRIGGER,
#endif
#ifdef SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER
    ENABLE_FTS3_TOKENIZER = SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER,
#endif
#ifdef SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION
    ENABLE_LOAD_EXTENSION = SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
#endif
#ifdef SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
    NO_CKPT_ON_CLOSE      = SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
#endif
    LOOKASIDE             = SQLITE_DBCONFIG_LOOKASIDE
};

static const char* decode_errcode(int c)
{
    switch (c)
    {
        case SQLITE_OK        : return "Successful result";
        case SQLITE_ERROR     : return "SQL error or missing database";
        case SQLITE_INTERNAL  : return "Internal logic error in SQLite";
        case SQLITE_PERM      : return "Access permission denied";
        case SQLITE_ABORT     : return "Callback routine requested an abort";
        case SQLITE_BUSY      : return "The database file is locked";
        case SQLITE_LOCKED    : return "A table in the database is locked";
        case SQLITE_NOMEM     : return "A malloc() failed";
        case SQLITE_READONLY  : return "Attempt to write a readonly database";
        case SQLITE_INTERRUPT : return "Operation terminated by sqlite3_interrupt()";
        case SQLITE_IOERR     : return "Some kind of disk I/O error occurred";
        case SQLITE_CORRUPT   : return "The database disk image is malformed";
        case SQLITE_NOTFOUND  : return "Unknown opcode in sqlite3_file_control()";
        case SQLITE_FULL      : return "Insertion failed because database is full";
        case SQLITE_CANTOPEN  : return "Unable to open the database file";
        case SQLITE_PROTOCOL  : return "Database lock protocol error";
        case SQLITE_EMPTY     : return "Database is empty";
        case SQLITE_SCHEMA    : return "The database schema changed";
        case SQLITE_TOOBIG    : return "String or BLOB exceeds size limit";
        case SQLITE_CONSTRAINT: return "Abort due to constraint violation";
        case SQLITE_MISMATCH  : return "Data type mismatch";
        case SQLITE_MISUSE    : return "Library used incorrectly";
        case SQLITE_NOLFS     : return "Uses OS features not supported on host";
        case SQLITE_AUTH      : return "Authorization denied";
        case SQLITE_FORMAT    : return "Auxiliary database format error";
        case SQLITE_RANGE     : return "2nd parameter to sqlite3_bind out of range";
        case SQLITE_NOTADB    : return "File opened that is not a database file";
#ifdef SQLITE_NOTICE
        case SQLITE_NOTICE    : return "Notifications from sqlite3_log()";
#endif
#ifdef SQLITE_WARNING
        case SQLITE_WARNING   : return "Warnings from sqlite3_log()";
#endif
        case SQLITE_ROW       : return "sqlite3_step() has another row ready";
        case SQLITE_DONE      : return "sqlite3_step() has finished executing";
    }
    return "UNKNOWN";
}

// forward declaration
class driver;
class statement;
class connection;



//=====================================================================================


/**
 * result_set - is a class that implements dbi::iresult_set interface and
 * represents results of SQL query execution with collection of helper function
 * to make the retrieval of the results of a query execution an easy task.
 * result_set object cannot be instantiated directly, only via statement execute()
 * function call. result_set objects automatically cancel executed
 * queries and destroy allocated memory as they go out of scope.
 */
class result_set : public dbi::iresult_set
{
public:
    void clear()
    {
        stepped = false;
        name2index.clear();
        row_cnt = 0;
        column_cnt = 0;
        affected_rows = 0;
    }

    virtual bool has_data()
    {
        return column_count();
    }

    virtual bool more_results()
    {
        return (stmts_index > 0 && (stmts_index < sqlite_stmts.size() || (stmts_index == sqlite_stmts.size() && stepped)));
    }

    virtual size_t row_count() const
    {
        return std::abs(row_cnt);
    }

    virtual size_t rows_affected() const
    {
        return affected_rows;
    }

    virtual size_t column_count() const
    {
        return column_cnt;
    }

    virtual std::string column_name(size_t col_idx)
    {
        validate();
        return sqlite3_column_name(sqlite_stmt, col_idx);
    }

    virtual int column_index(const std::string& col_name)
    {
        auto it = name2index.find(col_name);
        if (it != name2index.end())
            return it->second;
        return -1;
    }

    virtual bool prev()
    {
        throw std::runtime_error(std::string(__FUNCTION__).append(": Cursors are not supported by database"));
    }

    virtual bool first()
    {
        throw std::runtime_error(std::string(__FUNCTION__).append(": Cursors are not supported by database"));
    }

    virtual bool last()
    {
        throw std::runtime_error(std::string(__FUNCTION__).append(": Cursors are not supported by database"));
    }

    virtual bool next()
    {
        validate();
        if (stepped && column_cnt > 0)
        {
            stepped = false;
            return true;
        }
        auto res = sqlite3_step(sqlite_stmt);
        switch (res)
        {
            case SQLITE_DONE:
                affected_rows = (row_cnt > 0 ? row_cnt : sqlite3_changes(sqlite_conn));
                sqlite3_reset(sqlite_stmt);
                if (stmts_index > 0 && stmts_index < sqlite_stmts.size())
                {
                    name2index.clear();
                    row_cnt = column_cnt = 0;
                    sqlite_stmt = sqlite_stmts[stmts_index++];
                    int tmp = affected_rows;
                    next();
                    stepped = true;
                    affected_rows = tmp;
                }
                break;
            case SQLITE_ROW:
                {
                    row_cnt += 1;
                    if (0 == column_cnt)
                    {
                        column_cnt = sqlite3_column_count(sqlite_stmt);
                        for (auto i = 0; i < column_cnt; ++i)
                            name2index[sqlite3_column_name(sqlite_stmt, i)] = i;
                    }
                }
                return true;
            case SQLITE_BUSY:
                cancel();
                break;
            default:
                cancel();
                throw std::runtime_error(std::string(__FUNCTION__).append(": ").append(decode_errcode(res)));
        }
        return false;
    }

    virtual bool is_null(size_t col_idx)
    {
        validate();
        return (SQLITE_NULL == sqlite3_column_type(sqlite_stmt, col_idx));
    }

    virtual int16_t get_short(size_t col_idx)
    {
        return get_slint<int16_t>(col_idx);
    }

    virtual uint16_t get_ushort(size_t col_idx)
    {
        return get_slint<uint16_t>(col_idx);
    }

    virtual int32_t get_int(size_t col_idx)
    {
        return get_slint<int32_t>(col_idx);
    }

    virtual uint32_t get_uint(size_t col_idx)
    {
        return get_slint<uint32_t>(col_idx);
    }

    virtual int64_t get_long(size_t col_idx)
    {
        return get_slint64<int64_t>(col_idx);
    }

    virtual uint64_t get_ulong(size_t col_idx)
    {
        return get_slint64<uint64_t>(col_idx);
    }

    virtual float get_float(size_t col_idx)
    {
        return static_cast<float>(get_double(col_idx));
    }

    virtual double get_double(size_t col_idx)
    {
        validate();
        return sqlite3_column_double(sqlite_stmt, col_idx);
    }

    virtual bool get_bool(size_t col_idx)
    {
        return get_slint<bool>(col_idx);
    }

    virtual char get_char(size_t col_idx)
    {
        validate();
        return *sqlite3_column_text(sqlite_stmt, col_idx);
    }

    virtual std::string get_string(size_t col_idx)
    {
        validate();
        auto start = reinterpret_cast<const char*>(sqlite3_column_text(sqlite_stmt, col_idx));
        return std::move(std::string(start, sqlite3_column_bytes(sqlite_stmt, col_idx)));
    }

    virtual int get_date(size_t col_idx)
    {
        auto s = get_string(col_idx);
        s.erase(std::remove(s.begin(), s.end(), '-'), s.end());
        return std::stoi(s);
    }

    virtual double get_time(size_t col_idx)
    {
        auto s = get_string(col_idx);
        s.erase(std::remove(s.begin(), s.end(), ':'), s.end());
        return std::stod(s);
    }

    virtual time_t get_datetime(size_t col_idx)
    {
        std::string s = get_string(col_idx);
        std::memset(&stm, 0, sizeof(stm));
        std::sscanf(s.c_str(), "%04d-%02d-%02d %02d:%02d:%02d", &stm.tm_year, &stm.tm_mon, &stm.tm_mday, &stm.tm_hour, &stm.tm_min, &stm.tm_sec);
        stm.tm_year -= 1900;
        stm.tm_mon -= 1;
        stm.tm_isdst = -1;
        return mktime(&stm);
    }

    virtual char16_t get_u16char(size_t col_idx)
    {
        validate();
        return *(reinterpret_cast<const char16_t*>(sqlite3_column_text16(sqlite_stmt, col_idx)));
    }

    virtual std::u16string get_u16string(size_t col_idx)
    {
        validate();
        auto start = reinterpret_cast<const char16_t*>(sqlite3_column_text16(sqlite_stmt, col_idx));
        sqlite3_column_bytes16(sqlite_stmt, col_idx);
        return std::u16string(start);
    }

    virtual std::vector<uint8_t> get_binary(size_t col_idx)
    {
        validate();
        auto data = sqlite3_column_blob(sqlite_stmt, col_idx);
        std::vector<uint8_t> t(sqlite3_column_bytes(sqlite_stmt, col_idx));
        std::memcpy(reinterpret_cast<void*>(t.data()), data, t.size());
        return std::move(t);
    }

    bool cancel()
    {
        clear();
        if (nullptr != sqlite_conn)
            sqlite3_interrupt(sqlite_conn);
        if (nullptr != sqlite_stmt)
            return (SQLITE_OK == sqlite3_reset(sqlite_stmt));
        return true;
    }

private:
    friend class statement;

    result_set(std::vector<sqlite3_stmt*>& stmts) : sqlite_stmts(stmts) {}
    result_set(const result_set& rs) = delete;
    result_set& operator=(const result_set& rs) = delete;
    
    template<typename T>
    T get_slint(size_t col_idx)
    {
        validate();
        return static_cast<T>(sqlite3_column_int(sqlite_stmt, col_idx));
    }
    
    template<typename T>
    T get_slint64(size_t col_idx)
    {
        validate();
        return static_cast<T>(sqlite3_column_int64(sqlite_stmt, col_idx));
    }
    
    void validate()
    {
        if (sqlite_stmt == nullptr)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid result state object state"));
    }

private:
    bool stepped = false;
    long row_cnt = 0;
    long column_cnt = 0;
    size_t stmts_index = 0;
    size_t affected_rows = 0;
    sqlite3* sqlite_conn = nullptr;
    sqlite3_stmt* sqlite_stmt = nullptr;
    struct tm stm;
    std::vector<sqlite3_stmt*>& sqlite_stmts;
    std::map<std::string, int> name2index;
}; // result_set


//=====================================================================================


/**
 * sqlite driver class based on sqlite3 C++ library. This class cannot be
 * used directly and is intended to be used via wrapper dbd::driver class
 */
class driver : public idriver
{
public:    
    dbi::connection get_connection(const std::string& server);

    driver& version(long& ver)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        ver = sqlite3_libversion_number();
        return *this;
    }

    driver& version_string(std::string& ver)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        ver = sqlite3_libversion();
        return *this;
    }

    driver& max_connections(unsigned int conn_num)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        max_conn = conn_num;
        return *this;
    }

    template <typename... T>
    driver& config(config_flag flag, T... t)
    {
        int ret = SQLITE_OK;
        std::lock_guard<utils::spin_lock> lg(lock);
        if (flag == config_flag::SOFT_HEAP_LIMIT)
        {
            if (sizeof...(t) != 1)
                ret = SQLITE_MISUSE;
            else
            {
                int p = {t...};
                sqlite3_soft_heap_limit(p);
            }
        }
        else
        {
            if (conn_cnt > 0)
                throw std::runtime_error(std::string(__FUNCTION__).append(": This function must be used before any connections are opened"));
            sqlite3_shutdown();
            ret = sqlite3_config(utils::base_type(flag), t...);
            sqlite3_initialize();
        }
        if (SQLITE_OK != ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set config parameter: ").append(std::to_string(utils::base_type(flag))).append(": ").append(decode_errcode(ret)));
        return *this;
    }

protected:
    friend class connection;
    driver(const driver&) = delete;
    driver(driver&&) = delete;
    driver& operator=(const driver&) = delete;
    driver& operator=(driver&&) = delete;

    driver()
    {
    }
    
    void upd_conn_count(int change)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        conn_cnt += change;
    }
    
    bool is_max_conn()
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        return conn_cnt == max_conn;
    }

private:
    unsigned int max_conn = UINT_MAX;
    unsigned int conn_cnt = 0;
    utils::spin_lock lock;
}; // driver


//=====================================================================================

/**
 * connection - is a class that implements dbi::iconnection interface and
 * represents native database connection handle, in case of sqlite it's a C++
 * wrap around around sqlite3.
 * connection object cannot be instantiated directly, only via driver
 * get_connection() function call. connection objects automatically delete the
 * native connection handle they manage as soon as they themselves are destroyed.
 */
class connection : public dbi::iconnection
{
public:
    virtual ~connection()
    {
        disconnect();
    }

    connection(connection&& conn)
        : sqlite_conn(conn.sqlite_conn), is_utf16(conn.is_utf16),
        is_autocommit(conn.is_autocommit), oflag(conn.oflag),
        vfsname(std::move(conn.vfsname)), server(std::move(conn.server))
    {
        conn.sqlite_conn = nullptr;
    }

    connection& operator=(connection&& conn)
    {
        if (this != &conn)
        {
            disconnect();
            sqlite_conn = conn.sqlite_conn;
            conn.sqlite_conn = nullptr;
            is_utf16 = conn.is_utf16;
            is_autocommit = conn.is_autocommit;
            oflag = conn.oflag;
            vfsname = std::move(conn.vfsname);
            server = std::move(conn.server);
        }
        return *this;
    }

    template <typename... T>
    connection& config(db_config_flag flag, T... t)
    {
        if (sqlite_conn == nullptr)
            throw std::runtime_error(std::string(__FUNCTION__).append(": This function must be used after connection is opened"));
        int ret = sqlite3_db_config(sqlite_conn, utils::base_type(flag), t...);
        if (SQLITE_OK != ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set config parameter: ").append(std::to_string(utils::base_type(flag))).append(": ").append(decode_errcode(ret)));
        return *this;
    }

    virtual bool connect()
    {
        if (drv->is_max_conn())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't open new connections, please revise max connections"));
        
        if (true == connected())
            disconnect();
        if (oflag == 0)
        {
            if (SQLITE_OK != (is_utf16 ? sqlite3_open16(server.c_str(), &sqlite_conn) : sqlite3_open(server.c_str(), &sqlite_conn)))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to connect: ").append(server));
        }
        else
        {
            if (SQLITE_OK != sqlite3_open_v2(server.c_str(), &sqlite_conn, oflag, (vfsname.empty() ? nullptr : vfsname.c_str())))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to connect: ").append(server));
        }
        drv->upd_conn_count(1);
        return alive();
    }

    virtual void disconnect()
    {
        if (nullptr != sqlite_conn)
        {
            sqlite3_close(sqlite_conn);
            sqlite_conn = nullptr;
            drv->upd_conn_count(-1);
        }
    };

    virtual bool connected() const
    {
        return (nullptr != sqlite_conn);
    }

    virtual bool alive() const
    {
        return (nullptr != sqlite_conn);
    }
    
    virtual void autocommit(bool ac)
    {
        if (nullptr != sqlite_conn)
        {
            if (ac != is_autocommit)
            {
                is_autocommit = ac;
                if (ac)
                    sqlite_exec("rollback transaction;");
                else
                    sqlite_exec("begin transaction;");
            }
        }
    }

    virtual void commit()
    {
        if (false == is_autocommit)
        {
            sqlite_exec("commit transaction;");
            sqlite_exec("begin transaction;");
        }
    }

    virtual void rollback()
    {
        if (false == is_autocommit)
        {
            sqlite_exec("rollback transaction;");
            sqlite_exec("begin transaction;");
        }
    }

    virtual dbi::istatement* get_statement(dbi::iconnection& iconn);

    connection& flags(open_flag flag)
    {
        oflag = utils::base_type(flag);
        return *this;
    }

    connection& vfs(const std::string& name)
    {
        vfsname = name;
        return *this;
    }

    connection& utf16(bool flag)
    {
        is_utf16 = flag;
        return *this;
    }

    sqlite3* native_connection() const
    {
        return sqlite_conn;
    }
    
private:
    void sqlite_exec(const std::string& sql)
    {
        char* err = nullptr;
        if (SQLITE_OK != sqlite3_exec(sqlite_conn, sql.c_str(), nullptr, nullptr, &err))
        {
            std::string s = (err ? err : "unknown error");
            sqlite3_free(err);
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to execute: ").append(sql).append(": ").append(s));
        }
    }

private:
    friend class driver;
    friend class statement;

    connection() = delete;
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    connection(driver* drv, const std::string& server)
        : drv(drv), server(server)
    {
    }

private:
    driver* drv = nullptr;
    sqlite3* sqlite_conn = nullptr;
    bool is_utf16 = false;
    bool is_autocommit = true;
    int oflag = 0;
    std::string vfsname;
    std::string server;
}; // connection


dbi::connection driver::get_connection(const std::string& server)
{
    return create_connection(new connection(this, server));
}



//=====================================================================================



/**
 * statement - is a class that implements dbi::istatement interface and
 * represents native database statement structure, in case of sqlite it's a C++
 * wrap around sqlite3_stmt that also contains result set structure.
 * statement object cannot be instantiated directly, only via connection
 * get_statement() function call. statement objects automatically cancel executed
 * queries and destroy result set as they go out of scope.
 */
class statement : public dbi::istatement
{
public:
    ~statement()
    {
        cancel();
    }

    virtual bool cancel()
    {
        bool res = rs.cancel();
        for (auto stmt : sqlite_stmts)
        {
            if (SQLITE_OK != sqlite3_finalize(stmt))
                res = false;
        }
        sqlite_stmts.clear();
        rs.sqlite_stmt = nullptr;
        rs.stmts_index = 0;
        return res;
    }

    virtual dbi::iresult_set* execute()
    {
        rs.cancel();
        return fetch();
    }

    virtual dbi::iresult_set* execute(const std::string& cmd, bool usecursor = false, bool scrollable = false)
    {
        cancel();
        command = cmd;
        command.erase(std::find_if(command.rbegin(), command.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), command.end());
        size_t plen = 0;
        while (command.length() > 0 && plen != command.length())
        {
            command.erase(command.begin(), std::find_if(command.begin(), command.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
            sqlite_stmts.push_back(nullptr);
            prepare(command, &sqlite_stmts[sqlite_stmts.size() - 1]);
            plen = command.length();
            command = tail;
        }
        return fetch();
    }

    virtual void prepare(const std::string& cmd)
    {
        if (cmd.empty())
            throw std::runtime_error(std::string(__FUNCTION__).append(": SQL command is not set"));
        if (false == conn.alive())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Database connection is dead"));
        cancel();
        sqlite_stmts.resize(1);
        prepare(cmd, &sqlite_stmts[0]);
    }

    virtual void call(const std::string& cmd)
    {
        throw std::runtime_error(std::string(__FUNCTION__).append(": Stored procedures are not supported by database"));
    }
    
    virtual int proc_retval()
    {
        throw std::runtime_error(std::string(__FUNCTION__).append(": Stored procedures are not supported by database"));
    }
    
    virtual void set_null(size_t param_idx)
    {
        validate();
        if (SQLITE_OK != sqlite3_bind_null(sqlite_stmts.front(), param_idx + 1))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set null at index ").append(std::to_string(param_idx)));
    }
    
    virtual void set_short(size_t param_idx, int16_t val)
    {
        set_slint(param_idx, val);
    }
    
    virtual void set_ushort(size_t param_idx, uint16_t val)
    {
        set_slint(param_idx, val);
    }
    
    virtual void set_int(size_t param_idx, int32_t val)
    {
        set_slint(param_idx, val);
    }
    
    virtual void set_uint(size_t param_idx, uint32_t val)
    {
        set_int(param_idx, val);
    }
    
    virtual void set_long(size_t param_idx, int64_t val)
    {
        set_slint64(param_idx, val);
    }
    
    virtual void set_ulong(size_t param_idx, uint64_t val)
    {
        set_slint64(param_idx, val);
    }
    
    virtual void set_float(size_t param_idx, float val)
    {
        set_double(param_idx, val);
    }
    
    virtual void set_double(size_t param_idx, double val)
    {
        validate();
        if (SQLITE_OK != sqlite3_bind_double(sqlite_stmts.front(), param_idx + 1, val))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set double at index ").append(std::to_string(param_idx)));
    }
    
    virtual void set_bool(size_t param_idx, bool val)
    {
        set_slint(param_idx, val);
    }
    
    virtual void set_char(size_t param_idx, char val)
    {
        set_string(param_idx, std::string(1, val));
    }
    
    virtual void set_string(size_t param_idx, const std::string& val)
    {
        validate();
        if (SQLITE_OK != sqlite3_bind_text(sqlite_stmts.front(), param_idx + 1, val.c_str(), val.size(), SQLITE_TRANSIENT))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set string at index ").append(std::to_string(param_idx)));
    }
    
    virtual void set_date(size_t param_idx, int val)
    {
        auto yr = val / 10000;
        auto mon = (val % 10000) / 100;
        auto day = val % 100;
        std::vector<char> dt(11);
        std::sprintf(dt.data(), "%4d-%02d-%02d", yr, mon, day);
        set_string(param_idx, dt.data());
    }
    
    virtual void set_time(size_t param_idx, double val)
    {
        auto t = static_cast<int>(val);
        auto hr =  t / 10000;
        auto min = (t % 10000) / 100;
        auto sec = t % 100;
        auto ms = static_cast<int>(floor((val - t) * 1000 + 0.5)); 
        std::vector<char> dt(13);
        std::sprintf(dt.data(), "%02d:%02d:%02d.%03d", hr, min, sec, ms);
        set_string(param_idx, dt.data());
    }
    
    virtual void set_datetime(size_t param_idx, time_t val)
    {
#if defined(_WIN32) || defined(_WIN64)
        ::localtime_s(&stm, &val);
#else
        ::localtime_r(&val, &stm);
#endif
        stm.tm_year += 1900;
        std::vector<char> dt(20);
        std::sprintf(dt.data(), "%04d-%02d-%02d %02d:%02d:%02d", stm.tm_year, stm.tm_mon + 1, stm.tm_mday, stm.tm_hour, stm.tm_min, stm.tm_sec);
        set_string(param_idx, dt.data());
    }
    
    virtual void set_u16char(size_t param_idx, char16_t val)
    {
        set_u16string(param_idx, std::u16string(1, val));
    }
    
    virtual void set_u16string(size_t param_idx, const std::u16string& val)
    {
        validate();
        if (SQLITE_OK != sqlite3_bind_text16(sqlite_stmts.front(), param_idx + 1, val.data(), val.size() * sizeof(char16_t), SQLITE_TRANSIENT))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set text16 at index ").append(std::to_string(param_idx)));
    }
    
    virtual void set_binary(size_t param_idx, const std::vector<uint8_t>& val)
    {
        validate();
        if (SQLITE_OK != sqlite3_bind_blob(sqlite_stmts.front(), param_idx + 1, val.data(), val.size(), SQLITE_TRANSIENT))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set blob at index ").append(std::to_string(param_idx)));
    }

private:
    friend class connection;
    statement() = delete;
    statement(const statement&) = delete;
    statement& operator=(const statement&) = delete;
    statement(connection& conn) : conn(conn), rs(sqlite_stmts)
    {
        rs.sqlite_conn = conn.sqlite_conn;
    }
    
    template<typename T>
    void set_slint(size_t param_idx, T val)
    {
        validate();
        int ret = sqlite3_bind_int(sqlite_stmts.front(), param_idx + 1, val);
        if (SQLITE_OK != ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set int at index ").append(std::to_string(param_idx)).append(": ").append(decode_errcode(ret)));
    }
    
    template<typename T>
    void set_slint64(size_t param_idx, T val)
    {
        validate();
        int ret = sqlite3_bind_int64(sqlite_stmts.front(), param_idx + 1, val);
        if (SQLITE_OK != ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set int64 at index ").append(std::to_string(param_idx)).append(": ").append(decode_errcode(ret)));
    }

    void prepare(const std::string& cmd, sqlite3_stmt** stmtptr)
    {
        auto ret = sqlite3_prepare_v2(conn.sqlite_conn, cmd.c_str(), cmd.length(), stmtptr, &tail);
        if (SQLITE_OK != ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to prepare command, error code: ").append(decode_errcode(ret)));
    }
    dbi::iresult_set* fetch()
    {
        size_t rows_affected = 0;
        int failed_cnt = 0;
        std::string err = "";
        for (size_t i = 0; i < sqlite_stmts.size(); ++i)
        {
            rs.clear();
            rs.sqlite_stmt = sqlite_stmts[i];
            try
            {
                rs.next();
            }
            catch (const std::exception& e)
            {
                failed_cnt += 1;
                err.append(e.what()).append("; ");
            }
            rs.stepped = true;
            rows_affected += rs.rows_affected();
            if (rs.has_data())
            {
                rs.stmts_index = i + 1;
                break;
            }
        }
        rs.affected_rows = rows_affected;
        if (failed_cnt > 0)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to execute ").append(std::to_string(failed_cnt)).append(" command(s): ").append(err));
        return &rs;
    }
    
    void validate()
    {
        if (sqlite_stmts.size() == 0)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid statement object state"));
    }

private:
    const char* tail = nullptr;
    std::vector<sqlite3_stmt*> sqlite_stmts;
    connection& conn;
    bool cursor = false;
    std::string command;
    result_set rs;
    struct tm stm;
}; // statement


dbi::istatement* connection::get_statement(dbi::iconnection& iconn)
{
    return new statement(dynamic_cast<connection&>(iconn));
}






} } } } // namespace vgi::dbconn::dbd::sqlite

#endif // SQLITE_DRIVER_HPP

