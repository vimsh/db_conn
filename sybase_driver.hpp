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
 * to make the retrieval of the results of a query execution an easy task.
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
            data.resize(size, '\0');
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
        return std::abs(row_cnt);
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
        auto it = name2index.find(col_name);
        if (it != name2index.end())
            return it->second;
        return -1;
    }

    virtual bool prev()
    {
        return scroll_fetch(CS_PREV);
    }

    virtual bool first()
    {
        return scroll_fetch(CS_FIRST);
    }

    virtual bool last()
    {
        return scroll_fetch(CS_LAST);
    }

    virtual bool next()
    {
        if (columndata.size() > 0)
        {
            if (scrollable)
            {
                return scroll_fetch(CS_NEXT);
            }
            else
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
        switch (columns[col_idx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(col_idx);
            case CS_USHORT_TYPE:
                return get<CS_USHORT>(col_idx);
#ifdef CS_USMALLINT_TYPE
            case CS_USMALLINT_TYPE:
                return get<CS_USMALLINT>(col_idx);
#endif
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint)"));
        }
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
            case CS_USMALLINT_TYPE:
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
            case CS_USHORT_TYPE:
                return get<CS_USHORT>(col_idx);
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
            case CS_LONG_TYPE:
                return get<CS_LONG>(col_idx);
#ifdef CS_BIGINT_TYPE
            case CS_BIGINT_TYPE:
                return get<CS_BIGINT>(col_idx);
#endif
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return get<int64_t>(col_idx);
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
                return get<uint64_t>(col_idx);
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
            case CS_MONEY_TYPE:
            case CS_MONEY4_TYPE:
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return get<double>(col_idx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float, numeric/decimal)"));
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
            case CS_BOUNDARY_TYPE:
            case CS_SENSITIVITY_TYPE:
#ifdef CS_XML_TYPE
            case CS_XML_TYPE:
#endif
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
            case CS_BOUNDARY_TYPE:
            case CS_SENSITIVITY_TYPE:
#ifdef CS_XML_TYPE
            case CS_XML_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: char, varchar, text)"));
        }
        return std::move(std::string(columndata[col_idx]));
    }

    virtual int get_date(size_t col_idx)
    {
#ifdef CS_TIME_TYPE
        if (CS_TIME_TYPE == columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
#endif
#ifdef CS_BIGTIME_TYPE
        if (CS_BIGTIME_TYPE == columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
#endif
        getdt(col_idx);
        return daterec.dateyear * 10000 + (daterec.datemonth + 1) * 100 + daterec.datedmonth;
    }

    virtual double get_time(size_t col_idx)
    {
#ifdef CS_DATE_TYPE
        if (CS_DATE_TYPE == columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: time, datetime, smalldatetime, bigdatetime)"));
#endif
        getdt(col_idx);
        return (double)(daterec.datehour * 10000 + daterec.dateminute * 100 + daterec.datesecond) + (double)daterec.datemsecond / 1000.0;
    }

    virtual time_t get_datetime(size_t col_idx)
    {
#ifdef CS_TIME_TYPE
        if (CS_TIME_TYPE == columns[col_idx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
#endif
        getdt(col_idx);
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

    virtual char16_t get_u16char(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_UNICHAR_TYPE:
#ifdef CS_UNITEXT_TYPE
            case CS_UNITEXT_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type: ").append(std::to_string(columns[col_idx].datatype)));
        }
        return get<CS_UNICHAR>(col_idx);
    }

    virtual std::u16string get_u16string(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_UNICHAR_TYPE:
#ifdef CS_UNITEXT_TYPE
            case CS_UNITEXT_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type: ").append(std::to_string(columns[col_idx].datatype)));
        }
        return std::u16string(reinterpret_cast<char16_t*>((char*)columndata[col_idx]));
    }

    virtual std::vector<uint8_t> get_binary(size_t col_idx)
    {
        switch (columns[col_idx].datatype)
        {
            case CS_IMAGE_TYPE:
            case CS_BINARY_TYPE:
            case CS_VARBINARY_TYPE:
            case CS_LONGBINARY_TYPE:
#ifdef CS_BLOB_TYPE
            case CS_BLOB_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type: ").append(std::to_string(columns[col_idx].datatype)));
        }
        std::vector<uint8_t> t(columndata[col_idx].length);
        std::memcpy(reinterpret_cast<void*>(t.data()), columndata[col_idx], columndata[col_idx].length);
        return std::move(t);
    }

    bool cancel()
    {
        if (CS_SUCCEED != ct_cancel(nullptr, cscommand, CS_CANCEL_ALL))
            return false;
        return true;
    }

private:
    friend class statement;

    result_set() {}
    result_set(const result_set&) = delete;
    result_set& operator=(const result_set&) = delete;
    
    void set_scrollable(bool scroll)
    {
        scrollable = scroll;
    }

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
        case CS_ROW_RESULT:
        case CS_CURSOR_RESULT:
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
    
    bool scroll_fetch(CS_INT type)
    {
        if (scrollable)
        {
            switch (ct_scroll_fetch(cscommand, type, CS_UNUSED, CS_TRUE, &result))
            {
                case CS_END_DATA:
                case CS_CURSOR_BEFORE_FIRST:
                    row_cnt = 0;
                    return false;
                case CS_CURSOR_AFTER_LAST:
                    row_cnt += 1;
                    return false;
            }
            switch (type)
            {
                case CS_FIRST: row_cnt = 1; break;
                case CS_LAST: row_cnt = -1; break;
                case CS_PREV: row_cnt -= 1; break;
                case CS_NEXT: row_cnt += 1; break;
            }
            return true;
        }
        else
            throw std::runtime_error(std::string(__FUNCTION__).append(": The function can only be called if scrollable cursor is used "));
    }

    template<typename T>
    T get(size_t col_idx)
    {
        if (is_null(col_idx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        switch (columns[col_idx].datatype)
        {
            case CS_NUMERIC_TYPE:
            case CS_DECIMAL_TYPE:
            case CS_MONEY_TYPE:
            case CS_MONEY4_TYPE:
            {
                double num = 0.0;
                std::memset(&destfmt, 0, sizeof(destfmt));
                destfmt.maxlength = sizeof(num);
                destfmt.datatype = CS_FLOAT_TYPE;
                destfmt.format = CS_FMT_UNUSED;
                destfmt.locale = nullptr;
                if (CS_SUCCEED != cs_convert(cscontext, &columns[col_idx], static_cast<CS_VOID*>(columndata[col_idx]), &destfmt, &num, 0))
                    throw std::runtime_error(std::string(__FUNCTION__).append(": cs_convert failed"));
                return num;
            }
        }
        if (sizeof(T) < columndata[col_idx].data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Sybase data type is larger than the primitive data type"));
        return *(reinterpret_cast<T*>((char*)columndata[col_idx]));
    }

    void getdt(size_t col_idx)
    {
        if (is_null(col_idx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        switch (columns[col_idx].datatype)
        {
            case CS_DATETIME_TYPE:
            case CS_DATETIME4_TYPE:
#ifdef CS_DATE_TYPE
            case CS_DATE_TYPE:
#endif
#ifdef CS_TIME_TYPE
            case CS_TIME_TYPE:
#endif
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
    bool scrollable = false;
    bool do_cancel = false;
    long row_cnt = 0;
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
}; // result_set



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
        : ase(conn.ase), is_autocommit(conn.is_autocommit),
          cscontext(conn.cscontext), csconnection(conn.csconnection),
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
            ase = conn.ase;
            is_autocommit = conn.is_autocommit;
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
            is_server_ase();
        return alive();
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

    Context* native_context() const
    {
        return cscontext;
    }

    Connection* native_connection() const
    {
        return csconnection;
    }
    
    bool is_ase()
    {
        return ase;
    }

private:
    friend class driver;
    friend class statement;

    connection() = delete;
    connection(const connection&) = delete;
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
    
    void is_server_ase()
    {
        std::unique_ptr<dbi::istatement> stmt(get_statement(*this));
        dbi::iresult_set* rs = stmt->execute("if object_id('dbo.sysobjects') is not null and object_id('dbo.syscolumns') is not null select 1 else select 0");
        if (!rs->next())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get server type"));
        ase = rs->get_int(0);
    }

private:
    bool ase = false;
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
        destroy(cscontext);
    }

    dbi::connection get_connection(const std::string& server, const std::string& user = "", const std::string& passwd = "")
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
        if (cslocale != nullptr)
            cs_loc_drop(cscontext, cslocale);
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
        cancel();
        close_cursor();
        ct_cmd_drop(cscommand);
    }

    virtual bool cancel()
    {
        return rs.cancel();
    }

    virtual dbi::iresult_set* execute()
    {
        if (false == conn.alive())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Database connection is dead"));
        if (CS_SUCCEED != ct_send(cscommand))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to send command: ").append(command));
        rs.next_result();
        return &rs;
    }

    virtual dbi::iresult_set* execute(const std::string& cmd, bool usecursor = false, bool scrollable = false)
    {
        if (cmd.empty())
            throw std::runtime_error(std::string(__FUNCTION__).append(": SQL command is not set"));
        set_command(cmd, CS_LANG_CMD);
        usecursor && init_cursor(scrollable) || init_command();
        return execute();
    }

    virtual void prepare(const std::string& cmd)
    {
        set_command(cmd, CS_LANG_CMD);
        std::string csid = genid("proc");
        if (false == conn.alive())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Database connection is dead"));
        if (CS_SUCCEED != ct_dynamic(cscommand, CS_PREPARE, const_cast<CS_CHAR*>(csid.c_str()), CS_NULLTERM, const_cast<CS_CHAR*>(command.c_str()), CS_NULLTERM))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to prepare command"));
        execute();
        if (CS_SUCCEED != ct_dynamic(cscommand, CS_DESCRIBE_INPUT, const_cast<CS_CHAR*>(csid.c_str()), CS_NULLTERM, nullptr, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get input params decription"));
        execute();
        if (CS_SUCCEED != ct_dynamic(cscommand, CS_EXECUTE, const_cast<CS_CHAR*>(csid.c_str()), CS_NULLTERM, nullptr, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set command for execution"));
        param_datafmt.resize(rs.columns.size());
        param_data.resize(rs.columns.size());
        for (auto i = 0U; i < rs.columns.size(); ++i)
        {
            param_datafmt[i] = rs.columns[i];
            param_data[i].allocate(rs.columns[i].maxlength);
            if (CS_SUCCEED != ct_setparam(cscommand, &(param_datafmt[i]), param_data[i], &(param_data[i].length), &(param_data[i].indicator)))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set param, index ").append(std::to_string(i)));
        }
    }

    virtual void call(const std::string& cmd)
    {
        get_proc_params(cmd);;
        set_command(cmd, CS_RPC_CMD);
        if (CS_SUCCEED != ct_command(cscommand, CS_RPC_CMD, const_cast<CS_CHAR*>(cmd.c_str()), CS_NULLTERM, CS_NO_RECOMPILE))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set stored proc command"));
        for (auto i = 0U; i < param_datafmt.size(); ++i)
        {
            if (CS_SUCCEED != ct_setparam(cscommand, &(param_datafmt[i]), param_data[i], &(param_data[i].length), &(param_data[i].indicator)))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set param, index ").append(std::to_string(i)));
        }
    }
    
    virtual int proc_retval()
    {
        if (CS_RPC_CMD == cmdtype && rs.more_results() && rs.next() && rs.column_count() == 1)
        {
            int ret = rs.get_int(0);
            rs.more_results() && rs.next();
            rs.more_results() && rs.next() && rs.cancel();
            return ret;
        }
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid function call, it must be called after all result sets from stored procedure are processed"));
    }
    
    virtual void set_null(size_t param_idx)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        param_data[param_idx].length = 0;
        param_data[param_idx].indicator = -1;
    }
    
    virtual void set_short(size_t param_idx, int16_t val)
    {
        set(param_idx, val);
    }
    
    virtual void set_ushort(size_t param_idx, uint16_t val)
    {
        set(param_idx, val);
    }
    
    virtual void set_int(size_t param_idx, int32_t val)
    {
        set(param_idx, val);
    }
    
    virtual void set_uint(size_t param_idx, uint32_t val)
    {
        set(param_idx, val);
    }
    
    virtual void set_long(size_t param_idx, int64_t val)
    {
        set(param_idx, val);
    }
    
    virtual void set_ulong(size_t param_idx, uint64_t val)
    {
        set(param_idx, val);
    }
    
    virtual void set_float(size_t param_idx, float val)
    {
        set(param_idx, val);
    }
    
    virtual void set_double(size_t param_idx, double val)
    {
        set(param_idx, val);
    }
    
    virtual void set_bool(size_t param_idx, bool val)
    {
        set(param_idx, val);
    }
    
    virtual void set_char(size_t param_idx, char val)
    {
        set(param_idx, val);
    }
    
    virtual void set_string(size_t param_idx, const std::string& val)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        param_data[param_idx].length = val.length();
        if (CS_TEXT_TYPE == param_datafmt[param_idx].datatype)
        {
            param_datafmt[param_idx].maxlength = param_data[param_idx].length;
            param_data[param_idx].allocate(param_data[param_idx].length);
        }
        if (param_data[param_idx].length > param_datafmt[param_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        param_data[param_idx].indicator = 0;
        std::memcpy(param_data[param_idx], val.c_str(), val.length());
    }
    
    virtual void set_date(size_t param_idx, int val)
    {
        int yr = val / 10000;
        int mon = (val % 10000) / 100;
        int day = val % 100;
        std::vector<char> dt(22);
        std::sprintf(dt.data(), "%4d%02d%02d 00:00:00.000", yr, mon, day);
        setdt(param_idx, dt.data());
    }
    
    virtual void set_time(size_t param_idx, double val)
    {
        int t = static_cast<int>(val);
        int hr =  t / 10000;
        int min = (t % 10000) / 100;
        int sec = t % 100;
        int ms = floor((val - t) * 1000 + 0.5); 
        std::vector<char> dt(22);
        std::sprintf(dt.data(), "19000101 %02d:%02d:%02d.%03d", hr, min, sec, ms);
        setdt(param_idx, dt.data());
    }
    
    virtual void set_datetime(size_t param_idx, time_t val)
    {
#if defined(_WIN32) || defined(_WIN64)
        ::localtime_s(&stm, &val);
#else
        ::localtime_r(&val, &stm);
#endif
        stm.tm_year += 1900;
        std::vector<char> dt(22);
        std::sprintf(dt.data(), "%04d%02d%02d %02d:%02d:%02d.000", stm.tm_year, stm.tm_mon + 1, stm.tm_mday, stm.tm_hour, stm.tm_min, stm.tm_sec);
        setdt(param_idx, dt.data());
    }
    
    virtual void set_u16char(size_t param_idx, char16_t val)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        param_data[param_idx].length = sizeof(char16_t);
        if (param_data[param_idx].length > param_datafmt[param_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        param_data[param_idx].indicator = 0;
        std::memcpy(param_data[param_idx], &val, param_data[param_idx].length);
    }
    
    virtual void set_u16string(size_t param_idx, const std::u16string& val)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        param_data[param_idx].length = sizeof(char16_t) * val.length();
#ifdef CS_UNITEXT_TYPE
        if (CS_UNITEXT_TYPE == param_datafmt[param_idx].datatype)
        {
            param_datafmt[param_idx].maxlength = param_data[param_idx].length;
            param_data[param_idx].allocate(param_data[param_idx].length);
        }
#endif  
        if (param_data[param_idx].length > param_datafmt[param_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        param_data[param_idx].indicator = 0;
        std::memcpy(param_data[param_idx], &val[0], param_data[param_idx].length);
    }
    
    virtual void set_binary(size_t param_idx, const std::vector<uint8_t>& val)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        param_data[param_idx].length = val.size();
        if (CS_IMAGE_TYPE == param_datafmt[param_idx].datatype || CS_LONGBINARY_TYPE == param_datafmt[param_idx].datatype)
        {
            param_datafmt[param_idx].maxlength = param_data[param_idx].length;
            param_data[param_idx].allocate(param_data[param_idx].length);
        }
        if (param_data[param_idx].length > param_datafmt[param_idx].maxlength)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Data length is greater than maximum field size"));
        param_data[param_idx].indicator = 0;
        std::memcpy(param_data[param_idx], &val[0], param_data[param_idx].length);
    }

private:
    friend class connection;
    statement() = delete;
    statement(const statement&) = delete;
    statement& operator=(const statement&) = delete;
    statement(connection& conn) : conn(conn)
    {
        if (CS_SUCCEED != ct_cmd_alloc(conn.csconnection, &cscommand))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate command struct"));
        rs.cscontext = conn.cscontext;
        rs.cscommand = cscommand;
    }
    
    std::string genid(const std::string& type)
    {
        static std::atomic_int cnt(1);
        std::string id = type;
        id.append(std::to_string(cnt++)).append(std::to_string(std::hash<std::string>()(command)));
        return id;
    }
    
    void set_command(const std::string& cmd, CS_INT type)
    {
        command = cmd;
        cmdtype = type;
        rs.cancel();
        rs.set_scrollable(false);
        close_cursor();
    }
    
    bool init_command()
    {
        if (CS_SUCCEED != ct_command(cscommand, CS_LANG_CMD, const_cast<CS_CHAR*>(command.c_str()), CS_NULLTERM, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set command"));
        return true;
    }
    
    bool init_cursor(bool scrollable)
    {
        cursor = true;
        rs.set_scrollable(scrollable);
        std::string csid = genid("cursor");
        if (CS_SUCCEED != ct_cursor(cscommand, CS_CURSOR_DECLARE, const_cast<CS_CHAR*>(csid.c_str()), CS_NULLTERM, const_cast<CS_CHAR*>(command.c_str()), CS_NULLTERM, (scrollable ? CS_SCROLL_CURSOR : CS_READ_ONLY)))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to declare cursor"));
        if (CS_SUCCEED != ct_cursor(cscommand, CS_CURSOR_OPEN, nullptr, CS_UNUSED, nullptr, CS_UNUSED, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to open cursor"));
        return true;
    }
    
    void close_cursor()
    {
        if (cursor)
        {
            ct_cursor(cscommand, CS_CURSOR_CLOSE, nullptr, CS_UNUSED, nullptr, CS_UNUSED, CS_DEALLOC);
            cursor = false;
            execute();
        }
    }
    
    void get_proc_params(std::string procname)
    {
        param_datafmt.clear();
        param_data.clear();
        std::string sql;
        if (conn.is_ase())
        {
            std::string procnum = "1";
            std::string sysprefix = "dbo";
            if (2 == std::count(procname.begin(), procname.end(), '.'))
                sysprefix.insert(0, procname.substr(0, procname.find('.') + 1));
            std::size_t pos = procname.find(';');
            if (std::string::npos != pos)        
            {
                procnum = procname.substr(pos + 1);
                procname.erase(pos);
            }
            sql = "select c.name, c.usertype, c.length, c.prec, c.scale, c.status2 from " +
                  sysprefix + ".sysobjects o, " + sysprefix + ".syscolumns c where o.id = object_id('" +
                  procname + "') and o.type = 'P' and o.id = c.id and c.number = " +
                  procnum + " order by c.colid";
        }
        else
        {
            sql = "select m.parm_name, m.domain_id, m.width, m.width, m.scale, case m.parm_mode_out when 'Y' then 2 else 1 end from sysobjects o, sysprocedure p, sysprocparm m where o.id = object_id('" + 
                  procname + "') and o.type = 'P' and o.name = p.proc_name and o.uid = p.creator and m.proc_id = p.proc_id and m.parm_type = 0 order by m.parm_id";
        }
        execute(sql);
        while (rs.next())
        {
            param_datafmt.push_back(CS_DATAFMT());
            CS_DATAFMT& datafmt = *param_datafmt.rbegin();
            memset(&datafmt, 0, sizeof(datafmt));
            strcpy(datafmt.name, rs.get_string(0).c_str());
            datafmt.namelen = CS_NULLTERM;
            datafmt.datatype = ctlib_datatype(rs.get_int(1));
            datafmt.format = CS_FMT_UNUSED;
            datafmt.maxlength = rs.get_int(2);
            if (!rs.is_null(3))
                datafmt.precision = rs.get_int(3);
            if (!rs.is_null(4))
                datafmt.scale = rs.get_int(4);
            datafmt.status = (rs.get_int(5) == 1 ? CS_INPUTVALUE : CS_RETURN);
            datafmt.locale = NULL;
            param_data.push_back(result_set::column_data());
            result_set::column_data& coldata = *param_data.rbegin();
            coldata.allocate(datafmt.maxlength);
            coldata.length = datafmt.maxlength;
        }
    }
    
    template<typename T>
    void set(size_t param_idx, T val)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        switch (param_datafmt[param_idx].datatype)
        {
            case CS_NUMERIC_TYPE:
            case CS_DECIMAL_TYPE:
            case CS_MONEY_TYPE:
            case CS_MONEY4_TYPE:
            {
                double t = val;
                std::memset(&srcfmt, 0, sizeof(srcfmt));
                srcfmt.datatype = CS_FLOAT_TYPE;
                srcfmt.format = CS_FMT_UNUSED;
                srcfmt.locale = nullptr;
                srcfmt.maxlength = sizeof(t);
                if (CS_SUCCEED != cs_convert(conn.cscontext, &srcfmt, &t, &param_datafmt[param_idx], param_data[param_idx], 0))
                    throw std::runtime_error(std::string(__FUNCTION__).append(": cs_convert failed"));
            }
            break;
            default:
            {
                if (sizeof(T) > param_data[param_idx].data.size() && CS_TINYINT_TYPE != param_datafmt[param_idx].datatype)
                    throw std::runtime_error(std::string(__FUNCTION__).append(": Primitive data type is larger than the Sybase data type"));
                *(reinterpret_cast<T*>((char*)param_data[param_idx])) = val;
            }
        }
        param_data[param_idx].length = param_datafmt[param_idx].maxlength;
        param_data[param_idx].indicator = 0;
    }

    void setdt(size_t param_idx, const std::string& val)
    {
        if (param_idx >= param_data.size())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid index"));
        std::memset(&srcfmt, 0, sizeof(srcfmt));
        srcfmt.datatype = CS_CHAR_TYPE;
        srcfmt.format = CS_FMT_UNUSED;
        srcfmt.locale = nullptr;
        srcfmt.maxlength = val.length();
        if (CS_SUCCEED != cs_convert(conn.cscontext, &srcfmt, const_cast<char*>(val.c_str()), &param_datafmt[param_idx], param_data[param_idx], 0))
            throw std::runtime_error(std::string(__FUNCTION__).append(": cs_convert failed"));
        param_data[param_idx].length = param_datafmt[param_idx].maxlength;
        param_data[param_idx].indicator = 0;
    }
    
    CS_INT ctlib_datatype(int dbt)
    {
        if (conn.is_ase())
        {
            switch(dbt)
            {
                case  1: return CS_CHAR_TYPE;      // char
                case  2: return CS_VARCHAR_TYPE;   // varchar
                case  3: return CS_BINARY_TYPE;    // binary
                case  4: return CS_VARBINARY_TYPE; // varbinary
                case  5: return CS_TINYINT_TYPE;   // tinyint
                case  6: return CS_SMALLINT_TYPE;  // smallint
                case  7: return CS_INT_TYPE;       // int
                case  8: return CS_FLOAT_TYPE;     // float
                case 10: return CS_NUMERIC_TYPE;   // numeric
                case 11: return CS_MONEY_TYPE;     // money
                case 12: return CS_DATETIME_TYPE;  // datetime
                case 13: return CS_INT_TYPE;       // intn
                case 14: return CS_FLOAT_TYPE;     // floatn
                case 15: return CS_DATETIME_TYPE;  // datetimn
                case 16: return CS_BIT_TYPE;       // bit
                case 17: return CS_MONEY_TYPE;     // moneyn
                case 19: return CS_TEXT_TYPE;      // text
                case 20: return CS_IMAGE_TYPE;     // image
                case 21: return CS_MONEY4_TYPE;    // smallmoney
                case 22: return CS_DATETIME4_TYPE; // smalldatetime
                case 23: return CS_REAL_TYPE;      // real
                case 24: return CS_CHAR_TYPE;      // nchar
                case 25: return CS_VARCHAR_TYPE;   // nvarchar
                case 26: return CS_DECIMAL_TYPE;   // decimal
                case 27: return CS_DECIMAL_TYPE;   // decimaln
                case 28: return CS_NUMERIC_TYPE;   // numericn
                case 34: return CS_UNICHAR_TYPE;   // unichar
                case 35: return CS_UNICHAR_TYPE;   // univarchar
#ifdef CS_UNITEXT_TYPE
                case 36: return CS_UNITEXT_TYPE;   // unitext
#endif
                case 37: return CS_DATE_TYPE;      // date
                case 38: return CS_TIME_TYPE;      // time
                case 39: return CS_DATE_TYPE;      // daten
                case 40: return CS_TIME_TYPE;      // timen
#ifdef CS_BIGINT_TYPE
                case 43: return CS_BIGINT_TYPE;    // bigint
#endif
#ifdef CS_USMALLINT_TYPE
                case 44: return CS_USMALLINT_TYPE; // usmallint
#endif
#ifdef CS_UINT_TYPE
                case 45: return CS_UINT_TYPE;      // uint
#endif
#ifdef CS_UBIGINT_TYPE
                case 46: return CS_UBIGINT_TYPE;   // ubigint
#endif
#ifdef CS_UINT_TYPE
                case 47: return CS_UINT_TYPE;      // uintn
#endif
                case 80: return CS_DATETIME_TYPE;  // timestamp
            }
        }
        else
        {
            switch (dbt)
            {
                case  1: return CS_SMALLINT_TYPE;  // smallint
                case  2: return CS_INT_TYPE;       // int
                case  3: return CS_NUMERIC_TYPE;   // numeric
                case  4: return CS_FLOAT_TYPE;     // float
                case  5: return CS_REAL_TYPE;      // real
                case  6: return CS_DATE_TYPE;      // date
                case  7: return CS_CHAR_TYPE;      // char
                case  8: return CS_CHAR_TYPE;      // char
                case  9: return CS_VARCHAR_TYPE;   // varchar
                case 10: return CS_LONGCHAR_TYPE;   // long varchar
                case 11: return CS_BINARY_TYPE;	   // binary
                case 12: return CS_LONGBINARY_TYPE;// longbinary
                case 13: return CS_DATETIME_TYPE;  // timestamp
                case 14: return CS_TIME_TYPE;      // time
                case 19: return CS_TINYINT_TYPE;   // tinyint
#ifdef CS_BIGINT_TYPE
                case 20: return CS_BIGINT_TYPE;    // bigint
#endif
#ifdef CS_UINT_TYPE
                case 21: return CS_UINT_TYPE;      // uint
#endif
#ifdef CS_USMALLINT_TYPE
                case 22: return CS_USMALLINT_TYPE; // usmallint
#endif
#ifdef CS_UBIGINT_TYPE
                case 23: return CS_UBIGINT_TYPE;   // ubigint
#endif
                case 24: return CS_BIT_TYPE;       // bit
                case 27: return CS_DECIMAL_TYPE;   // decimal
                case 28: return CS_VARBINARY_TYPE; // varbinary
            }
        }
        throw std::runtime_error(std::string(__FUNCTION__).append(": Unknown data type: ").append(std::to_string(dbt)));
    }

private:
    CS_COMMAND* cscommand = nullptr;
    connection& conn;
    bool cursor = false;
    CS_INT cmdtype = CS_RPC_CMD;
    std::string command;
    result_set rs;
    CS_DATAFMT srcfmt;
    struct tm stm;
    std::vector<CS_DATAFMT> param_datafmt;
    std::vector<result_set::column_data> param_data;
}; // statement



dbi::istatement* connection::get_statement(dbi::iconnection& iconn)
{
    return new statement(dynamic_cast<connection&>(iconn));
}






} } } } // namespace vgi::dbconn::dbd::sybase

#endif // SYBASE_DRIVER_HPP

