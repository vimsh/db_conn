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
#define	SYBASE_DRIVER_HPP

#include <ctpublic.h>
#include <cstring>
#include <vector>
#include <map>
#include "utilities.hpp"
#include "driver.hpp"

namespace vgi { namespace dbconn { namespace dbd { namespace sybase {

CS_INT TRUE = CS_TRUE;
CS_INT FALSE = CS_FALSE;

enum class cfg_type : char
{
    CT_LIB,
    CS_LIB
};

enum class cmd_type : CS_INT
{
    LANG_CMD = CS_LANG_CMD,
    MSG_CMD = CS_MSG_CMD,
    RPC_CMD = CS_RPC_CMD,
    SEND_DATA_CMD = CS_SEND_DATA_CMD,
    PACKAGE_CMD = CS_PACKAGE_CMD,
    SEND_BULK_CMD = CS_SEND_BULK_CMD
};

enum class action : CS_INT
{
    SET = CS_SET,
    GET = CS_GET,
    CLEAR = CS_CLEAR
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
public:
	class column_data
	{
	public:
		CS_INT length;
		CS_INT indicator; // indicator

		column_data() : length(0), indicator(0), data(0)
        { }

		void allocate(const size_t size)
		{
			data.resize(size + 1, '\0');
		}

        operator char*()
        {
            return data.data();
        }

	private:
		std::vector<CS_CHAR> data;
	};

public:
    void clear()
    {
        columns.clear();
        columndata.clear();
        name2index.clear();
        row_cnt = 0;
    }

    virtual bool has_data()
    {
        return (columns.size() > 0);
    }

    virtual bool more_results()
    {
        CS_INT res;
        bool done = false;
        clear();
        do_cancel = true;
        while (false == done)
        {
            retcode = ct_results(cscommand, &res);
            switch (retcode)
            {
            case CS_SUCCEED:
                done = process_ct_result(res);
                break;
            case CS_END_RESULTS:
            case CS_CANCELED:
                return false;
            case CS_FAIL:
                cancel();
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get results"));
            default:
                cancel();
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed ct_results returned unknown ret_code"));
            }
        }
        return true;
    }

    virtual size_t row_count() const
    {
        return row_cnt;
    }

    virtual size_t column_count() const
    {
        return columns.size();
    }

    virtual std::string column_name(size_t index)
    {
        return columns[index].name;
    }

    virtual int column_index(const std::string& col_name)
    {
        for (size_t n = 0; n < columns.size(); ++n)
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
        }
        return false;
    }

    virtual bool is_null(unsigned int colidx)
    {
        return (CS_NULLDATA == (CS_SMALLINT)columndata[colidx].indicator);
    }

    virtual int16_t get_short(unsigned int colidx)
    {
        if (CS_TINYINT_TYPE == columns[colidx].datatype)
            return get<CS_TINYINT>(colidx);
        if (CS_SMALLINT_TYPE == columns[colidx].datatype)
            return get<CS_SMALLINT>(colidx);
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, smallint)"));
    }

    virtual uint16_t get_ushort(unsigned int colidx)
    {
        if (CS_TINYINT_TYPE == columns[colidx].datatype)
            return get<CS_TINYINT>(colidx);
#ifdef CS_USMALLINT_TYPE
        if (CS_USMALLINT_TYPE == columns[colidx].datatype)
            return get<CS_USMALLINT>(colidx);
#endif
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint)"));
    }

    virtual int32_t get_int(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(colidx);
            case CS_SMALLINT_TYPE:
                return get<CS_SMALLINT>(colidx);
#ifdef CS_USMALLINT_TYPE
        if (CS_USMALLINT_TYPE == columns[colidx].datatype)
            return get<CS_USMALLINT>(colidx);
#endif
            case CS_INT_TYPE:
                return get<CS_INT>(colidx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, smallint, usmallint, int)"));
        }
    }

    virtual uint32_t get_uint(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(colidx);
#ifdef CS_USMALLINT_TYPE
            case CS_USMALLINT_TYPE:
                return get<CS_USMALLINT>(colidx);
#endif
#ifdef CS_UINT_TYPE
            case CS_UINT_TYPE:
                return get<CS_UINT>(colidx);
#endif
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint, uint)"));
        }
    }

    virtual int64_t get_long(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(colidx);
            case CS_SMALLINT_TYPE:
                return get<CS_SMALLINT>(colidx);
#ifdef CS_USMALLINT_TYPE
        if (CS_USMALLINT_TYPE == columns[colidx].datatype)
            return get<CS_USMALLINT>(colidx);
#endif
            case CS_INT_TYPE:
                return get<CS_INT>(colidx);
#ifdef CS_UINT_TYPE
            case CS_UINT_TYPE:
                return get<CS_UINT>(colidx);
#endif
#ifdef CS_BIGINT_TYPE
            case CS_BIGINT_TYPE:
                return get<CS_BIGINT>(colidx);
#endif
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<int64_t>(colidx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, smallint, usmallint, int, uint, bigint)"));
        }
    }

    virtual uint64_t get_ulong(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_TINYINT_TYPE:
                return get<CS_TINYINT>(colidx);
#ifdef CS_USMALLINT_TYPE
            case CS_USMALLINT_TYPE:
                return get<CS_USMALLINT>(colidx);
#endif
#ifdef CS_UINT_TYPE
            case CS_UINT_TYPE:
                return get<CS_UINT>(colidx);
#endif
#ifdef CS_UBIGINT_TYPE
            case CS_UBIGINT_TYPE:
                return get<CS_UBIGINT>(colidx);
#endif
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<uint64_t>(colidx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: tinyint, usmallint, uint, ubigint)"));
        }
    }

    virtual float get_float(unsigned int colidx)
    {
        if (CS_REAL_TYPE == columns[colidx].datatype)
            return get<CS_REAL>(colidx);
        if (CS_FLOAT_TYPE == columns[colidx].datatype && columndata[colidx].length <= 4)
            return get<CS_FLOAT>(colidx);
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float(p) if p < 16)"));
    }

    virtual double get_double(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_REAL_TYPE:
                return get<CS_REAL>(colidx);
            case CS_FLOAT_TYPE:
                return get<CS_FLOAT>(colidx);
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<double>(colidx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float, numeric/decimal)"));
        }
    }

    virtual long double get_ldouble(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_REAL_TYPE:
                return get<CS_REAL>(colidx);
            case CS_FLOAT_TYPE:
                return get<CS_FLOAT>(colidx);
            case CS_DECIMAL_TYPE:
            case CS_NUMERIC_TYPE:
                return getnum<long double>(colidx);
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: real, float, numeric, decimal)"));
        }
    }

    virtual bool get_bool(unsigned int colidx)
    {
        if (CS_BIT_TYPE != columns[colidx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: bit)"));
        return get<bool>(colidx);
    }

    virtual char get_char(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_CHAR_TYPE:
            case CS_LONGCHAR_TYPE:
            case CS_TEXT_TYPE:
            case CS_VARCHAR_TYPE:
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: char, varchar, text)"));
        }
        return get<char>(colidx);
    }

    virtual std::string get_string(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_CHAR_TYPE:
            case CS_LONGCHAR_TYPE:
            case CS_TEXT_TYPE:
            case CS_VARCHAR_TYPE:
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: char, varchar, text)"));
        }
        return std::string(columndata[colidx]);
    }

    virtual int get_date(unsigned int colidx)
    {
        if (CS_TIME_TYPE == columns[colidx].datatype
#ifdef CS_BIGTIME_TYPE
            || CS_BIGTIME_TYPE == columns[colidx].datatype
#endif
           )
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
        getdtrec(colidx);
        return daterec.dateyear * 10000 + (daterec.datemonth + 1) * 100 + daterec.datedmonth;
    }

    virtual double get_time(unsigned int colidx)
    {
        if (CS_DATE_TYPE == columns[colidx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: time, datetime, smalldatetime, bigdatetime)"));
        getdtrec(colidx);
        return (double)(daterec.datehour * 10000 + daterec.dateminute * 100 + daterec.datesecond) + (double)daterec.datemsecond / 1000.0;
    }

    virtual time_t get_datetime(unsigned int colidx)
    {
        if (CS_TIME_TYPE == columns[colidx].datatype)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type (supported: date, datetime, smalldatetime, bigdatetime)"));
        getdtrec(colidx);
        memset(&stm, 0, sizeof(stm));
        stm.tm_sec = daterec.datesecond;
        stm.tm_min = daterec.dateminute;
        stm.tm_hour = daterec.datehour;
        stm.tm_mon = daterec.datemonth;
        stm.tm_mday = daterec.datedmonth;
        stm.tm_year = daterec.dateyear - 1900;
        stm.tm_isdst = -1;
        return mktime(&stm);
    }

    virtual char16_t get_unichar(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_UNICHAR_TYPE:
#ifdef CS_UNITEXT_TYPE
            case CS_UNITEXT_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type"));
        }
        return get<CS_UNICHAR>(colidx);
    }

    virtual std::u16string get_unistring(unsigned int colidx)
    {
        switch(columns[colidx].datatype)
        {
            case CS_UNICHAR_TYPE:
#ifdef CS_UNITEXT_TYPE
            case CS_UNITEXT_TYPE:
#endif
                break;
            default:
                throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type"));
        }
        return std::u16string(reinterpret_cast<char16_t*>((char*)columndata[colidx]));
    }

    virtual std::vector<uint8_t> get_image(unsigned int colidx)
    {
        if (CS_IMAGE_TYPE == columns[colidx].datatype)
        {
            std::vector<uint8_t> t(columndata[colidx].length);
            memcpy(reinterpret_cast<void*>(t.data()), columndata[colidx], columndata[colidx].length);
            return t;
        }
        throw std::runtime_error(std::string(__FUNCTION__).append(": Invalid column data type"));
    }

    bool cancel()
    {
        if (true == do_cancel)
        {
            do_cancel = false;
            if (CS_SUCCEED != ct_cancel(NULL, cscommand, CS_CANCEL_CURRENT))
                return false;
        }
        return true;
    }

    bool cancel_all()
    {
        if (true == do_cancel) // TODO
        {
            do_cancel = false;
            if (CS_SUCCEED != ct_cancel(NULL, cscommand, CS_CANCEL_ALL))
                return false;
        }
        return true;
    }

private:
    friend class statement;

    result_set() : do_cancel(false), cscontext(nullptr), cscommand(nullptr), row_cnt(0)
    {
    }

    result_set(const result_set& rs) = delete;
    result_set& operator=(const result_set& rs) = delete;

    bool process_ct_result(CS_INT res)
    {
        switch (res)
        {
        case CS_CMD_DONE: // done processing one result set
            if (CS_SUCCEED != ct_res_info(cscommand, CS_ROW_COUNT, &res, CS_UNUSED, NULL))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get result row count"));
            row_cnt = res > 0 ? res : 0;
            return true;
        case CS_ROWFMT_RESULT:
            std::cout << "process_ct_result(): CS_ROWFMT_RESULT\n";
            if (CS_SUCCEED != ct_res_info(cscommand, CS_ROW_COUNT, &res, CS_UNUSED, NULL))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get result row count"));
            row_cnt = res > 0 ? res : 0;
            break;
        case CS_PARAM_RESULT:
        case CS_STATUS_RESULT:
        case CS_CURSOR_RESULT:
        case CS_ROW_RESULT:
            std::cout << "process_ct_result(): CS_ROW_RESULT\n";
            process_result(false);
            return true;
        case CS_COMPUTE_RESULT:
            std::cout << "process_ct_result(): CS_COMPUTE_RESULT\n";
            process_result(true);
            return true;
        case CS_COMPUTEFMT_RESULT:
        case CS_MSG_RESULT:
        case CS_DESCRIBE_RESULT:
        case CS_CMD_SUCCEED:
            break;
        case CS_CMD_FAIL:
            while (CS_SUCCEED == ct_results(cscommand, &res) && CS_CMD_DONE != res);
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to execute command"));
        default:
            throw std::runtime_error(std::string(__FUNCTION__).append(": Unknown return from ct_results: ").append(std::to_string(res)));
        }
        return false;
    }

    void process_result(bool compute)
    {
        CS_INT colcnt = 0;
        if (CS_SUCCEED != ct_res_info(cscommand, CS_NUMDATA, &colcnt, CS_UNUSED, NULL))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get number of columns"));
        if (colcnt <= 0)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Returned zero columns"));
        columns.resize(colcnt);
        columndata.resize(colcnt);
        CS_INT agg_op;
        for (CS_INT i = 0; i < colcnt; ++i)
        {
            if (CS_SUCCEED != ct_describe(cscommand, i + 1, &(columns[i])))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to get column description: index ").append(std::to_string(i)));
            if (compute)
            {
                if (CS_SUCCEED != ct_compute_info(cscommand, CS_COMP_OP, i + 1, &agg_op, CS_UNUSED, &(columns[i].namelen)))
                    throw std::runtime_error(std::string(__FUNCTION__).append(": Failed compute info call"));
                switch (agg_op)
                {
                    case CS_OP_SUM:   std::strcpy(columns[i].name, "sum");   break;
                    case CS_OP_AVG:   std::strcpy(columns[i].name, "avg");   break;
                    case CS_OP_COUNT: std::strcpy(columns[i].name, "count"); break;
                    case CS_OP_MIN:   std::strcpy(columns[i].name, "min");   break;
                    case CS_OP_MAX:   std::strcpy(columns[i].name, "max");   break;
                    default: std::strcpy(columns[i].name, "unknown"); break;
                }
            }
            name2index[columns[i].name] = i;
            columndata[i].allocate(columns[i].maxlength);
			if (CS_SUCCEED != ct_bind(cscommand, i + 1, &(columns[i]), columndata[i], &(columndata[i].length), (CS_SMALLINT *)&(columndata[i].indicator)))
                throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to bind column ").append(std::to_string(i)));
        }
    }

    void process_param_result()
    {

    }

    template<typename T>
    T get(unsigned int colidx)
    {
        if (is_null(colidx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        return *(reinterpret_cast<T*>((char*)columndata[colidx]));
    }

    template<typename T>
    T getnum(unsigned int colidx)
    {
        if (is_null(colidx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        double num = 0;
        memset(&destfmt, 0, sizeof(destfmt));
        destfmt.maxlength = sizeof(double);
        destfmt.datatype  = CS_FLOAT_TYPE;
        destfmt.format    = CS_FMT_UNUSED;
        destfmt.locale    = NULL;
        if (CS_SUCCEED != cs_convert(cscontext, &columns[colidx], (CS_VOID*)columndata[colidx], &destfmt, &num, 0))
            throw std::runtime_error(std::string(__FUNCTION__).append(": cs_convert failed"));
        return num;
    }

    void getdtrec(unsigned int colidx)
    {
        if (is_null(colidx))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Can't convert NULL data"));
        switch(columns[colidx].datatype)
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
        memset(&daterec, 0, sizeof(daterec));
        if (CS_SUCCEED != cs_dt_crack(cscontext, columns[colidx].datatype, (CS_VOID*)columndata[colidx], &daterec))
            throw std::runtime_error(std::string(__FUNCTION__).append(": cs_dt_crack failed"));
    }

private:
    bool do_cancel;
    CS_CONTEXT* cscontext;
    CS_COMMAND* cscommand;
    CS_DATAFMT destfmt;
    CS_DATEREC daterec;
    struct tm stm;
    std::map<std::string, int> name2index;
    std::vector<CS_DATAFMT> columns;
    std::vector<column_data> columndata;
    size_t row_cnt;
    CS_RETCODE retcode;
    CS_INT result;
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
        : cscontext(conn.cscontext), csconnection(conn.csconnection), server(std::move(conn.server)), user(std::move(conn.user)), passwd(std::move(conn.passwd))
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
        if (nullptr != csconnection && CS_SUCCEED == ct_connect(csconnection, (server.empty() ? nullptr : (CS_CHAR*)server.c_str()), server.empty() ? 0 : CS_NULLTERM))
            return connected();
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

    virtual void commit() // TODO
    {
        /*
        statement stmt = get_statement();
        stmt.execute("commit tran");
        // and start new transaction if not in auto-commit mode
        if (false == auto_commit())
            stmt.execute("begin tran");
        */
    }

    virtual void rollback() // TODO
    {
        /*
        statement stmt = get_statement();
        stmt.execute("rollback tran");
        // and start new transaction if not in auto-commit mode
        if (false == auto_commit())
            stmt.execute("begin tran");
        */
    }

    virtual dbi::istatement* get_statement(dbi::iconnection& iconn);

    connection& props(action actn, CS_INT property, CS_VOID* buffer, CS_INT buflen = CS_UNUSED, CS_INT* outlen = nullptr)
    {
        auto ret = false;
        CS_INT act = utils::base_type(actn);
        if (nullptr != csconnection && nullptr != buffer)
            ret = (CS_SUCCEED == ct_con_props(csconnection, act, property, buffer, buflen, outlen));
        if (false == ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection property"));
        return *this;
    }

    CS_CONTEXT* cs_context() const
    {
        return cscontext;
    }

    CS_CONNECTION* cs_connection() const
    {
        return csconnection;
    }

private:
    friend class driver;

    connection(const driver&) = delete;
    connection& operator=(const connection&) = delete;

    connection(CS_CONTEXT* context, CS_INT dbg_flag, const std::string& protofile, const std::string& server, const std::string& user, const std::string& passwd, const std::string& appname)
        : cscontext(context), csconnection(nullptr), server(server), user(user), passwd(passwd)
    {
        if (CS_SUCCEED != ct_con_alloc(cscontext, &csconnection))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate connection struct"));
        if (CS_SUCCEED != ct_con_props(csconnection, CS_SET, CS_USERNAME, (CS_CHAR*)user.c_str(), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection user"));
        if (CS_SUCCEED != ct_con_props(csconnection, CS_SET, CS_PASSWORD, (CS_CHAR*)passwd.c_str(), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection password"));
        if (false == appname.empty() && CS_SUCCEED != ct_con_props(csconnection, CS_SET, CS_APPNAME, (CS_CHAR*)appname.c_str(), CS_NULLTERM, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set connection name"));
        if (false == protofile.empty() && CS_SUCCEED != ct_debug(nullptr, csconnection, CS_SET_PROTOCOL_FILE, CS_UNUSED, (CS_CHAR*)protofile.c_str(), protofile.length()))
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
    CS_CONTEXT* cscontext;
    CS_CONNECTION* csconnection;
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

    dbi::connection get_connection(const std::string& server, const std::string& user, const std::string& passwd, const std::string& appname = "")
    {
        return create_connection(new connection(cscontext, dbg_flag, protofile, server, user, passwd, appname));
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
        if (nullptr == cscontext || CS_SUCCEED != ct_debug(cscontext, nullptr, CS_SET_DBG_FILE, CS_UNUSED, (CS_CHAR*)fname.c_str(), fname.length()))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set debug file name"));
        return *this;
    }

    driver& debug_protocol_file(const std::string& fname)
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        protofile = fname;
        return *this;
    }

    driver& config(action actn, cfg_type type, CS_INT property, CS_VOID* buffer, CS_INT buflen = CS_UNUSED, CS_INT* outlen = nullptr)
    {
        auto ret = false;
        CS_INT act = utils::base_type(actn);
        if (nullptr != cscontext && nullptr != buffer)
        {
            std::lock_guard<utils::spin_lock> lg(lock);
            if (cfg_type::CS_LIB == type)
                ret = (CS_SUCCEED == cs_config(cscontext, act, property, buffer, buflen, outlen));
            else
                ret = (CS_SUCCEED == ct_config(cscontext, act, property, buffer, buflen, outlen));
        }
        if (false == ret)
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set config parameter"));
        return *this;
    }

    driver& cs_msg_callback(CS_RETCODE (*func) (CS_CONTEXT*, CS_CLIENTMSG*))
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || nullptr == func ||
            CS_SUCCEED != cs_config(cscontext, CS_SET, CS_MESSAGE_CB, reinterpret_cast<CS_VOID*>(func), CS_UNUSED, nullptr))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set cs library callback function"));
        return *this;
    }

    driver& ct_msg_callback(CS_RETCODE (*func) (CS_CONTEXT*, CS_CONNECTION*, CS_CLIENTMSG*))
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || nullptr == func ||
            CS_SUCCEED != ct_callback(cscontext, nullptr, CS_SET, CS_CLIENTMSG_CB, reinterpret_cast<CS_VOID*>(func)))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set client message callback function"));
        return *this;
    }

    driver& srv_msg_callback(CS_RETCODE (*func) (CS_CONTEXT*, CS_CONNECTION*, CS_SERVERMSG*))
    {
        std::lock_guard<utils::spin_lock> lg(lock);
        if (nullptr == cscontext || nullptr == func ||
            CS_SUCCEED != ct_callback(cscontext, nullptr, CS_SET, CS_SERVERMSG_CB, reinterpret_cast<CS_VOID*>(func)))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set server message callback function"));
        return *this;
    }

    CS_CONTEXT* cs_context() const
    {
        return cscontext;
    }

    static const char* decode_severity(CS_INT v)
    {
        if (v < 0 || severity.size() - 1 < (unsigned int)v)
            return "UNKNOWN";
        return severity[v];
    }

protected:
    driver(const driver&) = delete;
    driver(driver&&) = delete;
    driver& operator=(const driver&) = delete;
    driver& operator=(driver&&) = delete;
    driver** operator&() = delete;

    driver() : cscontext(nullptr), dbg_flag(0)
    {
        allocate(cscontext, version());
        cs_msg_callback([](CS_CONTEXT* context, CS_CLIENTMSG* msg)
        {
            std::cout << __FUNCTION__ << ": CS Library message: Severity - " << CS_SEVERITY(msg->msgnumber) << " (" << decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                    "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                    ", number -" << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << std::endl;
            if (msg->osstringlen > 0)
                std::cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << std::endl;
            return (CS_SUCCEED);
        });
        ct_msg_callback([](CS_CONTEXT* context, CS_CONNECTION* connection, CS_CLIENTMSG* msg)
        {
            std::cout << __FUNCTION__ << ": Open Client Message: Severity - " << CS_SEVERITY(msg->msgnumber) << " (" << decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                    "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                    ", number - " << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << std::endl;
            if (msg->osstringlen > 0)
                std::cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << std::endl;
            return CS_SUCCEED;
        });
        srv_msg_callback([](CS_CONTEXT* context, CS_CONNECTION* connection, CS_SERVERMSG* msg)
        {
            std::cout << __FUNCTION__ << ": Server message: " << (msg->svrnlen > 0 ? std::string("Server '").append(msg->svrname).append("': ") : "")
                    << (msg->proclen > 0 ? std::string("Procedure '").append(msg->proc).append("': ") : "") << "Severity - " << msg->severity
                    << ", state - " << msg->state << ", origin - " << msg->line << ", number - " << msg->msgnumber << ", message - " << msg->text << std::endl;
            return CS_SUCCEED;
        });
    }

    void allocate(CS_CONTEXT*& cs_context, CS_INT version)
    {
        if (CS_SUCCEED != cs_ctx_alloc(version, &cs_context))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate context struct"));
        if (CS_SUCCEED != ct_init(cs_context, version))
        {
            destroy(cs_context);
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to initialize context struct"));
        }
    }

    void destroy(CS_CONTEXT*& cs_context)
    {
        if (CS_SUCCEED != ct_exit(cs_context, CS_UNUSED))
            ct_exit(cs_context, CS_FORCE_EXIT);
        cs_ctx_drop(cs_context);
        cs_context = nullptr;
    }

    CS_INT version()
    {
        CS_INT version = CS_VERSION_100;
        CS_CONTEXT* cs_context = nullptr;
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

    CS_CONTEXT* cscontext;
    CS_INT dbg_flag;
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

    virtual dbi::iresult_set* execute()
    {
        rs.cancel_all();
        if (command.empty())
            throw std::runtime_error(std::string(__FUNCTION__).append(": SQL command is not set"));
        if (false == conn.alive())
            throw std::runtime_error(std::string(__FUNCTION__).append(": Database connection is dead"));
        if (CS_SUCCEED != ct_command(cscommand, CS_LANG_CMD, (CS_CHAR*)command.c_str(), CS_NULLTERM, CS_UNUSED))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to set command"));
        if (CS_SUCCEED != ct_send(cscommand))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to execute command: ").append(command));
        rs.cscontext = conn.cs_context();
        rs.cscommand = cscommand;
        rs.more_results();
        return &rs;
    }

    virtual dbi::iresult_set* execute(const std::string& cmd)
    {
        return execute(cmd, cmd_type::LANG_CMD);
    }

    dbi::iresult_set* execute(const std::string& cmd, cmd_type type)
    {
        command = cmd;
        ctype = utils::base_type(type);
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

    statement(connection& conn) : ctype(CS_LANG_CMD), conn(conn)
    {
        if (CS_SUCCEED != ct_cmd_alloc(conn.cs_connection(), &cscommand))
            throw std::runtime_error(std::string(__FUNCTION__).append(": Failed to allocate command struct"));
    }

private:
    CS_INT ctype;
    CS_COMMAND* cscommand;
    connection& conn;
    std::string command;
    result_set rs;
};



dbi::istatement* connection::get_statement(dbi::iconnection& iconn)
{
    return new statement(dynamic_cast<connection&>(iconn));
}






} } } } // namespace vgi::dbconn::dbd::sybase

#endif	// SYBASE_DRIVER_HPP

