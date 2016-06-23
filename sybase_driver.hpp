/*
 * File:   sybase_driver.hpp
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

#ifndef SYBASE_DRIVER_HPP
#define SYBASE_DRIVER_HPP

#include <ctpublic.h>
#include <cstring>
#include <vector>
#include <map>
#include <functional>
#include <algorithm>
#include <type_traits>
#include "utilities.hpp"
#include "driver.hpp"

namespace vgi { namespace dbconn { namespace dbd { namespace sybase {

static auto TRUE = CS_TRUE;
static auto FALSE = CS_FALSE;

using Context = CS_CONTEXT;
using Connection = CS_CONNECTION;
using ServerMessage = CS_SERVERMSG;
using ClientMessage = CS_CLIENTMSG;

enum class cfg_type : char
{
    CT_LIB,
    CS_LIB
};

enum class action : CS_INT
{
    SET = CS_SET,
    GET = CS_GET,
    CLEAR = CS_CLEAR,
    SUPPORTED = CS_SUPPORTED // only used for connection properties
};

enum class debug_flag : CS_INT
{
    ASYNC = CS_DBG_ASYNC,
    ERROR = CS_DBG_ERROR,
    MEM = CS_DBG_MEM,
    API_STATES = CS_DBG_API_STATES,
    NETWORK = CS_DBG_NETWORK,
    API_LOGCALL = CS_DBG_API_LOGCALL,
    ALL = CS_DBG_ALL,
    PROTOCOL = CS_DBG_PROTOCOL,
    PROTOCOL_STATES = CS_DBG_PROTOCOL_STATES,
    DIAG = CS_DBG_DIAG
};

constexpr debug_flag operator|(debug_flag l, debug_flag r) { return debug_flag(utils::base_type(l) | utils::base_type(r)); }



// forward declaration
class driver;
class statement;
class connection;



//=====================================================================================


/**
 * result_set - is a class that implements dbi::iresult_set interface and
 * represents results of SQL query execution with collection of helper function
 * to make the retrieval of the results of a sybase query execution an easy task.
 * result_set object cannot be instantiated directly, only via statement execute()
 * function call. result_set objects automatically cancel executed
 * queries and destroy allocated memory as they go out of scope.
 */
class result_set : public dbi::iresult_set
{
private:
    struct column_data
    {
        CS_INT length = 0;
        CS_SMALLINT indicator = 0;
        std::vector<CS_CHAR> data;

        void allocate(const size_t size)
        {
            data.resize(size + 1, '\0');
        }

        operator char*()
        {
            return data.data();
        }
    };

public:
    void clear()
    {
        columndata.clear();
        name2index.clear();
        row_cnt = 0;
        affected_rows = 0;
        more_res = false;
    }

    virtual bool has_data()
    {
        return (columns.size() > 0);
    }

    virtual bool more_results()
    {
        return more_res;
    }

    virtual size_t row_count() const
    {
        return row_cnt;
    }

    virtual size_t rows_affected() const
    {
        return affected_rows;
    }

    virtual size_t column_count() const
    {
        return columns.size();
    }

    virtual std::string column_name(size_t col_idx)
    {
        if (col_idx >= columns.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column index"));
        return columns[col_idx].name;
    }

    virtual int column_index(const std::string& col_name)
    {
        for (auto n = 0U; n < columns.size(); ++n)
        {
            if (0 == std::strcmp(col_name.c_str(), columns[n].name))
                return n;
        }
        return -1;
    }

    virtual bool next()
    {
        if (columndata.size() > 0)
        {
            if ((CS_SUCCEED == (retcode = ct_fetch(cscommand, CS_UNUSED, CS_UNUSED, CS_UNUSED, &result))) || CS_ROW_FAIL == retcode)
            {
                if (CS_ROW_FAIL == retcode)
                    throw std::runtime_error(std::string(__FUNCTION__).append(": Error fetching row ").append(std::to_string(result)));
                row_cnt += result;
                return true;
            }
            else
                next_result();
        }
        return false;
    }

    virtual bool is_null(size_t col_idx)
    {
        if (col_idx >= columns.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column index"));
        return (CS_NULLDATA == columndata[col_idx].indicator);
    }

    virtual int16_t get_short(size_t col_idx)
    {
        if (CS_TINYINT_TYPE == columns[col_idx].datatype)
            return get<CS_TINYINT>(col_idx);
        if (CS_SMALLINT_TYPE == columns[col_idx].datatype)
            return get<CS_SMALLINT>(col_idx);
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, smallint)"));
    }

    virtual uint16_t get_ushort(size_t col_idx)
    {
        if (CS_TINYINT_TYPE == columns[col_idx].datatype)
            return get<CS_TINYINT>(col_idx);
#ifdef CS_USMALLINT_TYPE
        if (CS_USMALLINT_TYPE == columns[col_idx].datatype)
            return get<CS_USMALLINT>(col_idx);
#endif
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint)"));
    }

    virtual int32_t get_int(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(col_idx);
            case CS_SMALLINT_TYPE:
                return get<CS_SMALLINT>(col_idx);
#ifdef CS_USMALLINT_TYPE
        if (CS_USMALLINT_TYPE == columns[col_idx].datatype)
            return get<CS_USMALLINT>(col_idx);
#endif
            case CS_INT_TYPE:
                return get<CS_INT>(col_idx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, smallint, usmallint, int)"));
        }
    }

    virtual uint32_t get_uint(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(col_idx);
#ifdef CS_USMALLINT_TYPE
            case CS_USMALLINT_TYPE:
                return get<CS_USMALLINT>(col_idx);
#endif
#ifdef CS_UINT_TYPE
            case CS_UINT_TYPE:
                return get<CS_UINT>(col_idx);
#endif
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint, uint)"));
        }
    }

    virtual int64_t get_long(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(col_idx);
            case CS_SMALLINT_TYPE:
                return get<CS_SMALLINT>(col_idx);
#ifdef CS_USMALLINT_TYPE
        if (CS_USMALLINT_TYPE == columns[col_idx].datatype)
            return get<CS_USMALLINT>(col_idx);
#endif
            case CS_INT_TYPE:
                return get<CS_INT>(col_idx);
#ifdef CS_UINT_TYPE
            case CS_UINT_TYPE:
                return get<CS_UINT>(col_idx);
#endif
#ifdef CS_BIGINT_TYPE
            case CS_BIGINT_TYPE:
                return get<CS_BIGINT>(col_idx);
#endif
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<int64_t>(col_idx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, smallint, usmallint, int, uint, bigint)"));
        }
    }

    virtual uint64_t get_ulong(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(col_idx);
#ifdef CS_USMALLINT_TYPE
            case CS_USMALLINT_TYPE:
                return get<CS_USMALLINT>(col_idx);
#endif
#ifdef CS_UINT_TYPE
            case CS_UINT_TYPE:
                return get<CS_UINT>(col_idx);
#endif
#ifdef CS_UBIGINT_TYPE
            case CS_UBIGINT_TYPE:
                return get<CS_UBIGINT>(col_idx);
#endif
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<uint64_t>(col_idx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint, uint, ubigint)"));
        }
    }

    virtual float get_float(size_t col_idx)
    {
        if (CS_REAL_TYPE == columns[col_idx].datatype)
            return get<CS_REAL>(col_idx);
        if (CS_FLOAT_TYPE == columns[col_idx].datatype && columndata[col_idx].length <= 4)
            return get<CS_FLOAT>(col_idx);
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float(p) if p < 16)"));
    }

    virtual double get_double(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_REAL_TYPE:
                return get<CS_REAL>(col_idx);
            case CS_FLOAT_TYPE:
                return get<CS_FLOAT>(col_idx);
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<double>(col_idx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float, numeric/decimal)"));
        }
    }

    virtual long double get_ldouble(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_REAL_TYPE:
                return get<CS_REAL>(col_idx);
            case CS_FLOAT_TYPE:
                return get<CS_FLOAT>(col_idx);
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<long double>(col_idx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float, numeric, decimal)"));
        }
    }

    virtual bool get_bool(size_t col_idx)
    {
        if (CS_BIT_TYPE != columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: bit)"));
        return get<bool>(col_idx);
    }

    virtual char get_char(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_CHAR_TYPE:
            case CS_LONGCHAR_TYPE:
            case CS_TEXT_TYPE:
            case CS_VARCHAR_TYPE:
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: char, varchar, text)"));
        }
        return get<char>(col_idx);
    }

    virtual std::string get_string(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_CHAR_TYPE:
            case CS_LONGCHAR_TYPE:
            case CS_TEXT_TYPE:
            case CS_VARCHAR_TYPE:
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: char, varchar, text)"));
        }
        return std::move(std::string(columndata[col_idx]));
    }

    virtual int get_date(size_t col_idx)
    {
        if (CS_TIME_TYPE == columns[col_idx].datatype
#ifdef CS_BIGTIME_TYPE
            || CS_BIGTIME_TYPE == columns[col_idx].datatype
#endif
           )
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
        getdtrec(col_idx);
        return daterec.dateyear * 10000 + (daterec.datemonth + 1) * 100 + daterec.datedmonth;
    }

    virtual double get_time(size_t col_idx)
    {
        if (CS_DATE_TYPE == columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: time, datetime, smalldatetime, bigdatetime)"));
        getdtrec(col_idx);
        return (double)(daterec.datehour * 10000 + daterec.dateminute * 100 + daterec.datesecond) + (double)daterec.datemsecond / 1000.0;
    }

    virtual time_t get_datetime(size_t col_idx)
    {
        if (CS_TIME_TYPE == columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
        getdtrec(col_idx);
        std::memset(&stm, 0, sizeof(stm));
        stm.tm_sec = daterec.datesecond;
        stm.tm_min = daterec.dateminute;
        stm.tm_hour = daterec.datehour;
        stm.tm_mon = daterec.datemonth;
        stm.tm_mday = daterec.datedmonth;
        stm.tm_year = daterec.dateyear - 1900;
        stm.tm_isdst = -1;
        return mktime(&stm);
    }

    virtual char16_t get_unichar(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_UNICHAR_TYPE:
#ifdef CS_UNITEXT_TYPE
            case CS_UNITEXT_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type"));
        }
        return get<CS_UNICHAR>(col_idx);
    }

    virtual std::u16string get_unistring(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_UNICHAR_TYPE:
#ifdef CS_UNITEXT_TYPE
            case CS_UNITEXT_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type"));
        }
        return std::u16string(reinterpret_cast<char16_t*>((char*)columndata[col_idx]));
    }

    virtual std::vector<uint8_t> get_image(size_t col_idx)
    {
        if (CS_IMAGE_TYPE == columns[col_idx].datatype)
        {
            std::vector<uint8_t> t(columndata[col_idx].length);
            std::memcpy(reinterpret_cast<void*>(t.data()), columndata[col_idx], columndata[col_idx].length);
            return t;
        }
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type"));
    }

    bool cancel()
    {
        if (true == do_cancel)
        {
            do_cancel = false;
            if (CS_SUCCEED != ct_cancel(nullptr, cscommand, CS_CANCEL_CURRENT))
                return false;
        }
        return true;
    }

    bool cancel_all()
    {
        if (true == do_cancel) // TODO
        {
            do_cancel = false;
            if (CS_SUCCEED != ct_cancel(nullptr, cscommand, CS_CANCEL_ALL))
                return false;
        }
        return true;
    }

private:
    friend class statement;

    result_set() {}
    result_set(const result_set& rs) = delete;
    result_set& operator=(const result_set& rs) = delete;
    

    bool next_result()
    {
        CS_INT res;
        auto done = false;
        auto failed_cnt = 0;
        clear();
        do_cancel = true;
        while (false == done)
        {
            retcode = ct_results(cscommand, &res);
            switch (retcode)
            {
            case CS_SUCCEED:
                done = process_ct_result(res);
                if (true == done)
                {
                    if (true == has_data())
                        more_res = true;
                    else
                        done = false;
                }
                else if (CS_CMD_FAIL == res)
                    failed_cnt += 1;
                break;
            case CS_END_RESULTS:
            case CS_CANCELED:
                done = true;
                break;
            case CS_FAIL:
                cancel();
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get results"));
            default:
                cancel();
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed ct_results returned unknown ret_code"));
            }
        }
        if (failed_cnt > 0)
        {
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to execute ").append(std::to_string(failed_cnt)).append(" command(s)"));
        }
        return more_res;
    }

    bool process_ct_result(CS_INT res)
    {
        switch (res)
        {
        case CS_PARAM_RESULT:
        case CS_STATUS_RESULT:
        case CS_CURSOR_RESULT:
        case CS_ROW_RESULT:
            process_result(false, true);
            return true;
        case CS_COMPUTE_RESULT:
            process_result(true, true);
            return true;
        case CS_DESCRIBE_RESULT:
            process_result(false, false);
            break;
        case CS_CMD_DONE:
            if (CS_SUCCEED != ct_res_info(cscommand, CS_ROW_COUNT, &res, CS_UNUSED, nullptr))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get result row count"));
            affected_rows += res > 0 ? res : 0;
            break;
        case CS_ROWFMT_RESULT: // not supported. seen only when the CS_EXPOSE_FORMATS property is enabled
        case CS_COMPUTEFMT_RESULT:// not supported. seen only when the CS_EXPOSE_FORMATS property is enabled
        case CS_MSG_RESULT: // not supported
            break;
        case CS_CMD_SUCCEED:
            break;
        case CS_CMD_FAIL:
            break;
        default:
            throw std::runtime_error(std::string(__FUNCTION__).append(": Unknown return from ct_results: ").append(std::to_string(res)));
        }
        return false;
    }

    void process_result(bool compute, bool bind)
    {
        auto colcnt = 0;
        if (CS_SUCCEED != ct_res_info(cscommand, CS_NUMDATA, &colcnt, CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get number of columns"));
        if (colcnt <= 0)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Returned zero columns"));
        std::vector<std::string> colnames;
        if (compute)
        {
            for (auto& dfmt : columns)
                colnames.push_back(dfmt.name);
        }
        columns.resize(colcnt);
        columndata.resize(colcnt);
        auto agg_op = 0;
        auto col_id = 0;
        for (auto i = 0; i < colcnt; ++i)
        {
            std::memset(&columns[i], 0, sizeof(CS_DATAFMT));
            if (CS_SUCCEED != ct_describe(cscommand, i + 1, &(columns[i])))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get column description: index ").append(std::to_string(i)));
            if (compute)
            {
                if (CS_SUCCEED != ct_compute_info(cscommand, CS_COMP_OP, i + 1, &agg_op, CS_UNUSED, &(columns[i].namelen)))
                    throw std::runtime_error(std::string(__FUNCTION__).append(": Failed compute info call for operation"));
                if (CS_SUCCEED != ct_compute_info(cscommand, CS_COMP_COLID, i + 1, &col_id, CS_UNUSED, &(columns[i].namelen)))
                    throw std::runtime_error(std::string(__FUNCTION__).append(": Failed compute info call for column id"));
                col_id -= 1;
                switch (agg_op)
                {
                    case CS_OP_SUM:   std::sprintf(columns[i].name, "sum(%s)",   colnames[col_id].c_str()); break;
                    case CS_OP_AVG:   std::sprintf(columns[i].name, "avg(%s)",   colnames[col_id].c_str()); break;
                    case CS_OP_COUNT: std::sprintf(columns[i].name, "count(%s)", colnames[col_id].c_str()); break;
                    case CS_OP_MIN:   std::sprintf(columns[i].name, "min(%s)",   colnames[col_id].c_str()); break;
                    case CS_OP_MAX:   std::sprintf(columns[i].name, "max(%s)",   colnames[col_id].c_str()); break;
                    default: std::sprintf(columns[i].name, "unknown(%s)", colnames[col_id].c_str()); break;
                }
            }
            else if (::strlen(columns[i].name) == 0)
                std::sprintf(columns[i].name, "column%d", i + 1);
            name2index[columns[i].name] = i;
            columndata[i].allocate(columns[i].maxlength);
            if (bind)
            {
                if (CS_SUCCEED != ct_bind(cscommand, i + 1, &(columns[i]), columndata[i], &(columndata[i].length), &(columndata[i].indicator)))
                    throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to bind column ").append(std::to_string(i)));
            }
        }
    }

    void process_param_result()
    {
        // TODO
    }

    template<typename T>
    T get(size_t col_idx)
    {
        if (is_null(col_idx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        return *(reinterpret_cast<T*>((char*)columndata[col_idx]));
    }

    template<typename T>
    T getnum(size_t col_idx)
    {
        if (is_null(col_idx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        auto num = 0.0D;
        std::memset(&destfmt, 0, sizeof(destfmt));
        destfmt.maxlength = sizeof(double);
        destfmt.datatype  = CS_FLOAT_TYPE;
        destfmt.format    = CS_FMT_UNUSED;
        destfmt.locale    = nullptr;
        if (CS_SUCCEED != cs_convert(cscontext, &columns[col_idx], static_cast<CS_VOID*>(columndata[col_idx]), &destfmt, &num, 0))
            throw std::runtime_error(std::string(__FUNCTION__).append(": cs_convert failed"));
        return num;
    }

    void getdtrec(size_t col_idx)
    {
        if (is_null(col_idx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        switch (columns[col_idx].datatype)
        {
            case CS_DATE_TYPE:
            case CS_DATETIME_TYPE:
            case CS_DATETIME4_TYPE:
            case CS_TIME_TYPE:
#ifdef CS_BIGDATETIME_TYPE
            case CS_BIGDATETIME_TYPE:
#endif
#ifdef CS_BIGTIME_TYPE
            case CS_BIGTIME_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, time, datetime, smalldatetime, bigdatetime)"));
        }
        std::memset(&daterec, 0, sizeof(daterec));
        if (CS_SUCCEED != cs_dt_crack(cscontext, columns[col_idx].datatype, static_cast<CS_VOID*>(columndata[col_idx]), &daterec))
            throw std::runtime_error(std::string(__FUNCTION__).append(": cs_dt_crack failed"));
    }

private:
    bool do_cancel = false;
    size_t row_cnt = 0;
    size_t affected_rows = 0;
    bool more_res = false;
    Context* cscontext = nullptr;
    CS_COMMAND* cscommand = nullptr;
    CS_RETCODE retcode;
    CS_INT result;
    CS_DATAFMT destfmt;
    CS_DATEREC daterec;
    struct tm stm;
    std::map<std::string, int> name2index;
    std::vector<CS_DATAFMT> columns;
    std::vector<column_data> columndata;
};



//=====================================================================================

/**
 * connection - is a class that implements dbi::iconnection interface and
 * represents native database connection handle, in case of Sybase it's a C++
 * wrap around around CS_CONTEXT and CS_CONNECTION.
 * connection object cannot be instantiated directly, only via driver
 * get_connection() function call. connection objects automatically delete the
 * native connection handle they manage as soon as they themselves are destroyed.
 */
class connection : public dbi::iconnection
{
public:
    virtual ~connection()
    {
        destroy();
    }

    connection(connection&& conn)
        : cscontext(conn.cscontext), csconnection(conn.csconnection),
          server(std::move(conn.server)), user(std::move(conn.user)),
          passwd(std::move(conn.passwd))
    {
        conn.cscontext = nullptr;
        conn.csconnection = nullptr;
    }

    connection& operator=(connection&& conn)
    {
        if (this != &conn)
        {
            destroy();
            cscontext = conn.cscontext;
            csconnection = conn.csconnection;
            conn.cscontext = nullptr;
            conn.csconnection = nullptr;
            server = std::move(conn.server);
            user = std::move(conn.user);
            passwd = std::move(conn.passwd);
        }
        return *this;
    }

    virtual bool connect()
    {
        if (true == connected())
            disconnect();
        if (nullptr != csconnection && CS_SUCCEED == ct_connect(csconnection, (server.empty() ? nullptr : const_cast<CS_CHAR*>(server.c_str())), server.empty() ? 0 : CS_NULLTERM))
            return alive();
        return false;
    }

    virtual void disconnect()
    {
        if (nullptr != csconnection)
        {
            CS_INT stat = 0;
            ct_con_props(csconnection, CS_GET, CS_CON_STATUS, &stat, CS_UNUSED, nullptr);
            if (CS_CONSTAT_DEAD == stat)
                ct_close(csconnection, CS_FORCE_CLOSE);
            else if (CS_CONSTAT_CONNECTED == stat)
            {
                ct_cancel(csconnection, nullptr, CS_CANCEL_ALL);
                if (CS_SUCCEED != ct_close(csconnection, CS_UNUSED))
                    ct_close(csconnection, CS_FORCE_CLOSE);
            }
        }
    };

    virtual bool connected() const
    {
        if (nullptr != csconnection)
        {
            CS_INT stat = 0;
            ct_con_props(csconnection, CS_GET, CS_CON_STATUS, &stat, CS_UNUSED, nullptr);
            return (CS_CONSTAT_CONNECTED == stat || CS_CONSTAT_DEAD == stat);
        }
        return false;
    }

    virtual bool alive() const
    {
        if (nullptr != csconnection)
        {
            CS_INT stat = 0;
            ct_con_props(csconnection, CS_GET, CS_CON_STATUS, &stat, CS_UNUSED, nullptr);
            return (CS_CONSTAT_CONNECTED == stat);
        }
        return false;
    }
    
    virtual void autocommit(bool ac)
    {
        if (nullptr != csconnection)
        {
            CS_BOOL val = (ac ? TRUE : FALSE);
            if (val != is_autocommit)
            {
                is_autocommit = val;
                std::unique_ptr<dbi::istatement> stmt(get_statement(*this));
                if (val)
                    stmt->execute("rollback tran");
                else
                    stmt->execute("begin tran");
            }
        }
    }

    virtual void commit()
    {
        if (FALSE == is_autocommit)
        {
            std::unique_ptr<dbi::istatement> stmt(get_statement(*this));
            stmt->execute("commit tran begin tran");
        }
    }

    virtual void rollback()
    {
        if (FALSE == is_autocommit)
        {
            std::unique_ptr<dbi::istatement> stmt(get_statement(*this));
            stmt->execute("rollback tran begin tran");
        }
    }

    virtual dbi::istatement* get_statement(dbi::iconnection& iconn);


    template<typename T>
    connection& userdata(T& user_struct)
    {
        T* us = &user_struct;
        if (nullptr == csconnection || CS_SUCCEED != ct_con_props(csconnection, CS_SET, CS_USERDATA, reinterpret_cast<CS_VOID*>(&us), sizeof(us), nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection user data pointer"));
        return *this;
    }

    template<typename T>
    connection& userdata(T*& user_struct)
    {
        auto len = 0;
        if (nullptr == csconnection || CS_SUCCEED != ct_con_props(csconnection, CS_GET, CS_USERDATA, reinterpret_cast<CS_VOID*>(&user_struct), sizeof(user_struct), &len))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get connection user data pointer"));
        return *this;
    }

    connection& props(action actn, CS_INT property, CS_VOID* buffer, CS_INT buflen = CS_UNUSED, CS_INT* outlen = nullptr)
    {
        if (nullptr == csconnection || nullptr == buffer || CS_SUCCEED != ct_con_props(csconnection, utils::base_type(actn), property, buffer, buflen, outlen))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection property"));
        return *this;
    }

    Context* cs_context() const
    {
        return cscontext;
    }

    Connection* cs_connection() const
    {
        return csconnection;
    }

private:
    friend class driver;

    connection(const driver&) = delete;
    connection& operator=(const connection&) = delete;

    connection(Context* context, CS_INT dbg_flag, const std::string& protofile, const std::string& server, const std::string& user, const std::string& passwd)
        : cscontext(context), server(server), user(user), passwd(passwd)
    {
        if (CS_SUCCEED != ct_con_alloc(cscontext, &csconnection))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate connection struct"));
        if (CS_SUCCEED != ct_con_props(csconnection, CS_SET, CS_USERNAME, const_cast<CS_CHAR*>(user.c_str()), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection user"));
        if (CS_SUCCEED != ct_con_props(csconnection, CS_SET, CS_PASSWORD, const_cast<CS_CHAR*>(passwd.c_str()), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection password"));
        if (false == protofile.empty() && CS_SUCCEED != ct_debug(nullptr, csconnection, CS_SET_PROTOCOL_FILE, CS_UNUSED, const_cast<CS_CHAR*>(protofile.c_str()), protofile.length()))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set debug protocol file name"));
        if (0 != dbg_flag && CS_SUCCEED != ct_debug(cscontext, csconnection, CS_SET_FLAG, dbg_flag, nullptr, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set debug flags"));
    }

    void destroy()
    {
        disconnect();
        if (nullptr != csconnection)
        {
            ct_con_drop(csconnection);
            csconnection = nullptr;
        }
        cscontext = nullptr;
    }

private:
    CS_BOOL is_autocommit = CS_TRUE;
    Context* cscontext = nullptr;
    Connection* csconnection = nullptr;
    std::string server;
    std::string user;
    std::string passwd;
};



//=====================================================================================


/**
 * sybase ASE driver class based on ctlib sybase C library. This class cannot be
 * used directly and is intended to be used via wrapper dbd::driver class
 */
class driver : public idriver
{
public:
    ~driver()
    {
        if (cslocale != nullptr)
            cs_loc_drop(cscontext, cslocale);
        destroy(cscontext);
    }

    dbi::connection get_connection(const std::string& server, const std::string& user, const std::string& passwd)
    {
        return create_connection(new connection(cscontext, dbg_flag, protofile, server, user, passwd));
    }

    driver& debug(debug_flag flag)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        dbg_flag = utils::base_type(flag);
        return *this;
    }

    driver& debug_file(const std::string& fname)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != ct_debug(cscontext, nullptr, CS_SET_DBG_FILE, CS_UNUSED, const_cast<CS_CHAR*>(fname.c_str()), fname.length()))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set debug file name"));
        return *this;
    }

    driver& debug_protocol_file(const std::string& fname)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        protofile = fname;
        return *this;
    }

    driver& app_name(const std::string& appname)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != cs_config(cscontext, CS_SET, CS_APPNAME, const_cast<CS_CHAR*>(appname.c_str()), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set application name"));
        return *this;
    }

    driver& config_file(const std::string& fname)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext ||
            CS_SUCCEED != cs_config(cscontext, CS_SET, CS_EXTERNAL_CONFIG, reinterpret_cast<CS_VOID*>(&TRUE), CS_UNUSED, nullptr) ||
            CS_SUCCEED != cs_config(cscontext, CS_SET, CS_CONFIG_FILE, const_cast<CS_CHAR*>(fname.c_str()), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set config file"));
        return *this;
    }

    driver& max_connections(unsigned int conn_num)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != ct_config(cscontext, CS_SET, CS_MAX_CONNECT, reinterpret_cast<CS_VOID*>(&conn_num), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set maximum connections"));
        return *this;
    }

    driver& timeout(unsigned int tout)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != ct_config(cscontext, CS_SET, CS_TIMEOUT, reinterpret_cast<CS_VOID*>(&tout), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection timeout"));
        return *this;
    }

    driver& keepalive(bool keep_alive)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != ct_config(cscontext, CS_SET, CS_CON_KEEPALIVE, reinterpret_cast<CS_VOID*>(&(keep_alive ? TRUE : FALSE)), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set keepalive property"));
        return *this;
    }

    driver& tcp_nodelay(bool nodelay)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != ct_config(cscontext, CS_SET, CS_CON_TCP_NODELAY, reinterpret_cast<CS_VOID*>(&(nodelay ? TRUE : FALSE)), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set keepalive property"));
        return *this;
    }

    driver& locale(const std::string& locale_name)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || (nullptr == cslocale && CS_SUCCEED != cs_loc_alloc(cscontext, &cslocale)))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate locale structure"));
        if (!locale_name.empty())
        {
            if (CS_SUCCEED != cs_locale(cscontext, CS_SET, cslocale, CS_LC_ALL, const_cast<CS_CHAR*>(locale_name.c_str()), CS_NULLTERM, nullptr) ||
                CS_SUCCEED != cs_config(cscontext, CS_SET, CS_LOC_PROP, reinterpret_cast<CS_VOID*>(cslocale), CS_UNUSED, nullptr))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set locale name"));
        }
        return *this;
    }

    template<typename T>
    driver& userdata(T& user_struct)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        T* us = &user_struct;
        if (nullptr == cscontext || CS_SUCCEED != cs_config(cscontext, CS_SET, CS_USERDATA, reinterpret_cast<CS_VOID*>(&us), sizeof(us), nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set context user data pointer"));
        return *this;
    }

    template<typename T>
    driver& userdata(T*& user_struct)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != cs_config(cscontext, CS_GET, CS_USERDATA, reinterpret_cast<CS_VOID*>(&user_struct), sizeof(user_struct), nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get context user data pointer"));
        return *this;
    }

    driver& version(long& ver)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || CS_SUCCEED != cs_config(cscontext,  CS_GET, CS_VERSION, reinterpret_cast<CS_VOID*>(&ver), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get sybase library version"));
        return *this;
    }

    driver& cs_msg_callback(CS_RETCODE (*func) (Context*, ClientMessage*))
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || nullptr == func ||
            CS_SUCCEED != cs_config(cscontext, CS_SET, CS_MESSAGE_CB, reinterpret_cast<CS_VOID*>(func), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set cs library callback function"));
        return *this;
    }

    driver& ct_msg_callback(CS_RETCODE (*func) (Context*, Connection*, ClientMessage*))
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || nullptr == func ||
            CS_SUCCEED != ct_callback(cscontext, nullptr, CS_SET, CS_CLIENTMSG_CB, reinterpret_cast<CS_VOID*>(func)))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set client message callback function"));
        return *this;
    }

    driver& srv_msg_callback(CS_RETCODE (*func) (Context*, Connection*, ServerMessage*))
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || nullptr == func ||
            CS_SUCCEED != ct_callback(cscontext, nullptr, CS_SET, CS_SERVERMSG_CB, reinterpret_cast<CS_VOID*>(func)))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set server message callback function"));
        return *this;
    }

    driver& config(action actn, cfg_type type, CS_INT property, CS_VOID* buffer, CS_INT buflen = CS_UNUSED, CS_INT* outlen = nullptr)
    {
        auto ret = false;
        if (nullptr != cscontext && nullptr != buffer)
        {
            std::lock_guard<utils::spin_lock> lg(lock);
            if (cfg_type::CS_LIB == type)
                ret = (CS_SUCCEED == cs_config(cscontext, utils::base_type(actn), property, buffer, buflen, outlen));
            else
                ret = (CS_SUCCEED == ct_config(cscontext, utils::base_type(actn), property, buffer, buflen, outlen));
        }
        if (false == ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set/get config parameter"));
        return *this;
    }

    Context* cs_context() const
    {
        return cscontext;
    }

    static const char* decode_severity(CS_INT v)
    {
        if (v < 0 || severity.size() - 1 < (size_t)v)
            return "UNKNOWN";
        return severity[v];
    }

protected:
    driver(const driver&) = delete;
    driver(driver&&) = delete;
    driver& operator=(const driver&) = delete;
    driver& operator=(driver&&) = delete;
    driver** operator&() = delete;

    driver()
    {
        allocate(cscontext, version());
        cs_msg_callback([](Context* context, ClientMessage* msg)
        {
            std::cout << __FUNCTION__ << ": CS Library message: Severity - " << CS_SEVERITY(msg->msgnumber) << " (" << decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                    "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                    ", number -" << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << std::endl;
            if (msg->osstringlen > 0)
                std::cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << std::endl;
            return (CS_SUCCEED);
        });
        ct_msg_callback([](Context* context, Connection* connection, ClientMessage* msg)
        {
            std::cout << __FUNCTION__ << ": Open Client Message: Severity - " << CS_SEVERITY(msg->msgnumber) << " (" << decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                    "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                    ", number - " << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << std::endl;
            if (msg->osstringlen > 0)
                std::cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << std::endl;
            return CS_SUCCEED;
        });
        srv_msg_callback([](Context* context, Connection* connection, ServerMessage* msg)
        {
            std::cout << __FUNCTION__ << ": Server message: " << (msg->svrnlen > 0 ? std::string("Server '").append(msg->svrname).append("': ") : "")
                    << (msg->proclen > 0 ? std::string("Procedure '").append(msg->proc).append("': ") : "") << "Severity - " << msg->severity
                    << ", state - " << msg->state << ", origin - " << msg->line << ", number - " << msg->msgnumber << ", message - " << msg->text << std::endl;
            return CS_SUCCEED;
        });
    }

    void allocate(Context*& cs_context, CS_INT version)
    {
        if (CS_SUCCEED != cs_ctx_alloc(version, &cs_context))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate context struct"));
        if (CS_SUCCEED != ct_init(cs_context, version))
        {
            destroy(cs_context);
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to initialize context struct"));
        }
    }

    void destroy(Context*& cs_context)
    {
        if (CS_SUCCEED != ct_exit(cs_context, CS_UNUSED))
            ct_exit(cs_context, CS_FORCE_EXIT);
        cs_ctx_drop(cs_context);
        cs_context = nullptr;
    }

    CS_INT version()
    {
        auto version = CS_VERSION_100;
        Context* cs_context = nullptr;
        try
        {
            allocate(cs_context, CS_VERSION_100);
            std::array<char, 256> verbuf = {'\0'};
            if (CS_SUCCEED == ct_config(cs_context, CS_GET, CS_VER_STRING, verbuf.data(), verbuf.size(), nullptr))
            {
                std::string verstr = {verbuf.begin(), verbuf.end()};
                auto pos = verstr.find("BUILD");
                if (std::string::npos != pos && (pos + 8) < verstr.length())
                {
                    auto ver = std::stoi(verstr.substr(pos + 5, 3));
#ifdef CS_VERSION_160
                    if (ver >= 160)
                        version = CS_VERSION_160;
                    else
#endif
#ifdef CS_VERSION_157
                    if (ver >= 157)
                        version = CS_VERSION_157;
                    else
#endif
#ifdef CS_VERSION_155
                    if (ver >= 155)
                        version = CS_VERSION_155;
                    else
#endif
#ifdef CS_VERSION_150
                    if (ver >= 150)
                        version = CS_VERSION_150;
                    else
#endif
#ifdef CS_VERSION_125
                    if (ver >= 125)
                        version = CS_VERSION_125;
                    else
#endif
#ifdef CS_VERSION_110
                    if (ver >= 110)
                        version = CS_VERSION_110;
#endif
                }
            }
            destroy(cs_context);
        }
        catch (...)
        { }
        return version;
    }

private:
    constexpr static std::array<const char*, 8> severity {{
        "CS_SV_INFORM",
        "CS_SV_CONFIG_FAIL",
        "CS_SV_RETRY_FAIL",
        "CS_SV_API_FAIL",
        "CS_SV_RESOURCE_FAIL",
        "CS_SV_COMM_FAIL",
        "CS_SV_INTERNAL_FAIL",
        "CS_SV_FATAL"
    }};

    CS_LOCALE* cslocale = nullptr;
    Context* cscontext = nullptr;
    CS_INT dbg_flag = 0;
    std::string protofile;
    utils::spin_lock lock;
}; // driver

constexpr std::array<const char*, 8> driver::severity;





//=====================================================================================



/**
 * statement - is a class that implements dbi::istatement interface and
 * represents native database statement structure, in case of Sybase it's a C++
 * wrap around CS_COMMAND that also contains result set structure .
 * statement object cannot be instantiated directly, only via connection
 * get_statement() function call. statement objects automatically cancel executed
 * queries and destroy result set as they go out of scope.
 */
class statement : public dbi::istatement
{
public:
    ~statement()
    {
        cancel_all();
        ct_cmd_drop(cscommand);
    }

    virtual void prepare(const std::string& cmd)
    {
        rs.cancel_all();
        command = cmd;
        static std::atomic_int cnt(1);
        std::string id = "p";
        id.append(std::to_string(cnt++)).append(std::to_string(std::hash<std::string>()(command)));
        auto csid = const_cast<CS_CHAR*>(id.c_str());
        if (false == conn.alive())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Database connection is dead"));
        if (CS_SUCCEED != ct_dynamic(cscommand, CS_PREPARE, csid, CS_NULLTERM, const_cast<CS_CHAR*>(command.c_str()), CS_NULLTERM))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to prepare command"));
        execute();
        if (CS_SUCCEED != ct_dynamic(cscommand, CS_DESCRIBE_INPUT, csid, CS_NULLTERM, nullptr, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get input params decription"));
        execute();
        if (CS_SUCCEED != ct_dynamic(cscommand, CS_EXECUTE, csid, CS_NULLTERM, nullptr, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set command for execution"));
        prep_datafmt.resize(rs.columns.size());
        prep_data.resize(rs.columns.size());
        for (auto i = 0U; i < rs.columns.size(); ++i)
        {
            prep_datafmt[i] = rs.columns[i];
            prep_data[i].allocate(rs.columns[i].maxlength);
            if (CS_SUCCEED != ct_setparam(cscommand, &(prep_datafmt[i]), prep_data[i], &(prep_data[i].length), &(prep_data[i].indicator)))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set param, index ").append(std::to_string(i)));
        }
    }
    
    virtual void set_null(size_t col_idx)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        prep_data[col_idx].length = 0;
        prep_data[col_idx].indicator = -1;
    }
    
    virtual void set_short(size_t col_idx, int16_t val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_ushort(size_t col_idx, uint16_t val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_int(size_t col_idx, int32_t val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_uint(size_t col_idx, uint32_t val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_long(size_t col_idx, int64_t val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_ulong(size_t col_idx, uint64_t val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_float(size_t col_idx, float val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_double(size_t col_idx, double val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_ldouble(size_t col_idx, long double val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_bool(size_t col_idx, bool val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_char(size_t col_idx, char val)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        prep_data[col_idx].indicator = 0;
        prep_data[col_idx].length = 1;
        prep_data[col_idx][0] = val;
    }
    
    virtual void set_string(size_t col_idx, const std::string& val)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        prep_data[col_idx].length = val.length();
        if (prep_data[col_idx].length > prep_datafmt[col_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        prep_data[col_idx].indicator = 0;
        std::memcpy(prep_data[col_idx], val.c_str(), val.length());
    }
    
    virtual void set_date(size_t col_idx, int val)
    {
        set(col_idx, std::to_string(val));
    }
    
    virtual void set_time(size_t col_idx, double val)
    {
        int t = static_cast<int>(val);
        int hr =  t / 10000;
        int min = (t % 10000) / 100;
        int sec = t % 100;
        int ms = (val - t) * 1000;
        std::vector<char> dt(23);
        std::sprintf(dt.data(), "1900-01-01 %02d:%02d:%02d.%03d", hr, min, sec, ms);
        set(col_idx, dt.data());
    }
    
    virtual void set_datetime(size_t col_idx, time_t val)
    {
#if defined(_WIN32) || defined(_WIN64)
        ::localtime_s(&stm, &val);
#else
        ::localtime_r(&val, &stm);
#endif
        stm.tm_year += 1900;
        std::vector<char> dt(23);
        std::sprintf(dt.data(), "%04d-%02d-%02d %02d:%02d:%02d.000", stm.tm_year, stm.tm_mon, stm.tm_mday, stm.tm_hour, stm.tm_min, stm.tm_sec);
        set(col_idx, dt.data());
    }
    
    virtual void set_unichar(size_t col_idx, char16_t val)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        prep_data[col_idx].length = sizeof(char16_t);
        if (prep_data[col_idx].length > prep_datafmt[col_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        prep_data[col_idx].indicator = 0;
        std::memcpy(prep_data[col_idx], &val, prep_data[col_idx].length);
    }
    
    virtual void set_unistring(size_t col_idx, const std::u16string& val)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        prep_data[col_idx].length = sizeof(char16_t) * val.length();
        if (prep_data[col_idx].length > prep_datafmt[col_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        prep_data[col_idx].indicator = 0;
        std::memcpy(prep_data[col_idx], &val[0], prep_data[col_idx].length);
    }
    
    virtual void set_image(size_t col_idx, const std::vector<uint8_t>& val)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        prep_data[col_idx].length = val.size();
        if (prep_data[col_idx].length > prep_datafmt[col_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        prep_data[col_idx].indicator = 0;
        std::memcpy(prep_data[col_idx], &val[0], prep_data[col_idx].length);
    }

    virtual dbi::iresult_set* execute()
    {
        if (false == conn.alive())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Database connection is dead"));
        if (CS_SUCCEED != ct_send(cscommand))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to send command: ").append(command));
        rs.cscontext = conn.cs_context();
        rs.cscommand = cscommand;
        rs.next_result();
        return &rs;
    }

    virtual dbi::iresult_set* execute(const std::string& cmd)
    {
        if (cmd.empty())
            throw std::runtime_error(std::string(__FUNCTION__).append(": SQL command is not set"));
        command = cmd;
        rs.cancel_all();
        if (CS_SUCCEED != ct_command(cscommand, CS_LANG_CMD, const_cast<CS_CHAR*>(command.c_str()), CS_NULLTERM, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set command"));
        return execute();
    }

    virtual bool cancel()
    {
        return rs.cancel();
    }

    virtual bool cancel_all()
    {
        return rs.cancel_all();
    }

private:
    friend class connection;
    statement() = delete;
    statement(connection& conn) : conn(conn)
    {
        std::memset(&srcfmt, 0, sizeof(srcfmt));
        srcfmt.datatype = CS_CHAR_TYPE;
        srcfmt.format = CS_FMT_NULLTERM;
        srcfmt.locale = nullptr;
        if (CS_SUCCEED != ct_cmd_alloc(conn.cs_connection(), &cscommand))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate command struct"));
    }

    void set(size_t col_idx, const std::string& val)
    {
        if (col_idx >= prep_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        srcfmt.maxlength = val.length();
        if (CS_SUCCEED != cs_convert(conn.cs_context(), &srcfmt, const_cast<char*>(val.c_str()), &prep_datafmt[col_idx], prep_data[col_idx], 0))
            throw std::runtime_error(std::string(__FUNCTION__).append(": cs_convert failed"));
        prep_data[col_idx].length = prep_datafmt[col_idx].maxlength;
        prep_data[col_idx].indicator = 0;
    }

private:
    CS_COMMAND* cscommand = nullptr;
    connection& conn;
    std::string command;
    result_set rs;
    CS_DATAFMT srcfmt;
    struct tm stm;
    std::vector<CS_DATAFMT> prep_datafmt;
    std::vector<result_set::column_data> prep_data;
};



dbi::istatement* connection::get_statement(dbi::iconnection& iconn)
{
    return new statement(dynamic_cast<connection&>(iconn));
}






} } } } // namespace vgi::dbconn::dbd::sybase

#endif // SYBASE_DRIVER_HPP

