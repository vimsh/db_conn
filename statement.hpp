/*
 * File:   statement.hpp
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

#ifndef STATEMENT_HPP
#define STATEMENT_HPP

#include "result_set.hpp"

namespace vgi { namespace dbconn { namespace dbi {

class connection;

/**
 * istatement - is an interface that describes common functionality for all
 * concrete native implementations for a statement class
 */
struct istatement
{
    virtual ~istatement() { }
    virtual void prepare(const std::string& sql) = 0;
    virtual void call(const std::string& sql) = 0;
    virtual iresult_set* execute(const std::string& sql, bool cursor = false, bool scrollable = false) = 0;
    virtual iresult_set* execute() = 0;
    virtual bool cancel() = 0;
    virtual int proc_retval() = 0;
    virtual void set_null(size_t col_idx) = 0;
    virtual void set_short(size_t col_idx, int16_t val) = 0;
    virtual void set_ushort(size_t col_idx, uint16_t val) = 0;
    virtual void set_int(size_t col_idx, int32_t val) = 0;
    virtual void set_uint(size_t col_idx, uint32_t val) = 0;
    virtual void set_long(size_t col_idx, int64_t val) = 0;
    virtual void set_ulong(size_t col_idx, uint64_t val) = 0;
    virtual void set_float(size_t col_idx, float val) = 0;
    virtual void set_double(size_t col_idx, double val) = 0;
    virtual void set_bool(size_t col_idx, bool val) = 0;
    virtual void set_char(size_t col_idx, char val) = 0;
    virtual void set_string(size_t col_idx, const std::string& val) = 0;
    virtual void set_date(size_t col_idx, int val) = 0;
    virtual void set_time(size_t col_idx, double val) = 0;
    virtual void set_datetime(size_t col_idx, time_t val) = 0;
    virtual void set_u16char(size_t col_idx, char16_t val) = 0;
    virtual void set_u16string(size_t col_idx, const std::u16string& val) = 0;
    virtual void set_binary(size_t col_idx, const std::vector<uint8_t>& val) = 0;
};


/**
 * statement - is a class that manages native driver statement handle.
 * statement object cannot be instantiated directly, only via connection
 * get_statement() function call. statement objects automatically delete the
 * native statement handle they manage as soon as they themselves are destroyed,
 * or as  soon as their value changes by an assignment operation. When an
 * assignment operation takes place between two statement objects, ownership of
 * native statement handle is transferred, which means that the object losing
 * ownership is no longer has access to the native statement handle.
 */
class statement
{
public:
    /**
     * Move constructor
     * @param stmt
     */
    statement(statement&& stmt) : stmt_impl(std::move(stmt.stmt_impl))
    {
    }

    /**
     * Assignment operator
     * @param stmt
     * @return 
     */
    statement& operator=(statement&& stmt)
    {
        if (this != &stmt)
            stmt_impl = std::move(stmt.stmt_impl);
        return *this;
    }

    /**
     * Function prepares dynamic SQL statement
     * @param sql statement to be executed
     */
    void prepare(const std::string& sql)
    {
        stmt_impl->prepare(sql);
    }

    /**
     * Function prepares SQL stored procedure
     * @param proc stored procedure to be executed
     */
    void call(const std::string& proc)
    {
        stmt_impl->call(proc);
    }

    /**
     * Function runs last executed SQL statement
     * @return result set object
     */
    result_set execute()
    {
        return result_set(stmt_impl->execute());
    }

    /**
     * Function runs SQL statement
     * @param sql statement to be executed
     * @param cursor = true to use cursor with select statements
     * @param scrollable = true to use scrollable cursor
     * @return result set object
     */
    result_set execute(const std::string& sql, bool cursor = false, bool scrollable = false)
    {
        return result_set(stmt_impl->execute(sql, cursor, scrollable));
    }

    /**
     * Function cancels currently running SQL statements
     * @return true if canceled, false otherwise
     */
    bool cancel()
    {
        return stmt_impl->cancel();
    }
    
    /**
     * Function returns stored procedure return value.
     * This function must be called after all result sets from stored proc select
     * statements had been processed
     * @return int
     */
    int proc_retval()
    {
        return stmt_impl->proc_retval();
    }
    
    virtual void set_null(size_t col_idx)
    {
        stmt_impl->set_null(col_idx);
    }
    
    virtual void set_short(size_t col_idx, int16_t val)
    {
        stmt_impl->set_short(col_idx, val);
    }
    
    virtual void set_ushort(size_t col_idx, uint16_t val)
    {
        stmt_impl->set_ushort(col_idx, val);
    }
    
    virtual void set_int(size_t col_idx, int32_t val)
    {
        stmt_impl->set_int(col_idx, val);
    }
    
    virtual void set_uint(size_t col_idx, uint32_t val)
    {
        stmt_impl->set_uint(col_idx, val);
    }
    
    virtual void set_long(size_t col_idx, int64_t val)
    {
        stmt_impl->set_long(col_idx, val);
    }
    
    virtual void set_ulong(size_t col_idx, uint64_t val)
    {
        stmt_impl->set_ulong(col_idx, val);
    }
    
    virtual void set_float(size_t col_idx, float val)
    {
        stmt_impl->set_float(col_idx, val);
    }
    
    virtual void set_double(size_t col_idx, double val)
    {
        stmt_impl->set_double(col_idx, val);
    }
    
    virtual void set_bool(size_t col_idx, bool val)
    {
        stmt_impl->set_bool(col_idx, val);
    }
    
    virtual void set_char(size_t col_idx, char val)
    {
        stmt_impl->set_char(col_idx, val);
    }
    
    virtual void set_string(size_t col_idx, const std::string& val)
    {
        stmt_impl->set_string(col_idx, val);
    }
    
    virtual void set_date(size_t col_idx, int val)
    {
        stmt_impl->set_date(col_idx, val);
    }
    
    virtual void set_time(size_t col_idx, double val)
    {
        stmt_impl->set_time(col_idx, val);
    }
    
    virtual void set_datetime(size_t col_idx, time_t val)
    {
        stmt_impl->set_datetime(col_idx, val);
    }
    
    virtual void set_u16char(size_t col_idx, char16_t val)
    {
        stmt_impl->set_u16char(col_idx, val);
    }
    
    virtual void set_u16string(size_t col_idx, const std::u16string& val)
    {
        stmt_impl->set_u16string(col_idx, val);
    }
    
    virtual void set_binary(size_t col_idx, const std::vector<uint8_t>& val)
    {
        stmt_impl->set_binary(col_idx, val);
    }
    
private:
    friend class connection;
    statement(istatement* stmt) : stmt_impl(stmt) { }
    statement(const statement&) = delete;
    statement& operator=(const statement&) = delete;

private:
    std::unique_ptr<istatement> stmt_impl;

}; // statement

} } } // namespace vgi::dbconn::dbi

#endif // STATEMENT_HPP

