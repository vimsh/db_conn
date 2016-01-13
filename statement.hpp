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
#define	STATEMENT_HPP

#include "result_set.hpp"

namespace vgi { namespace dbconn { namespace dbi {

class connection;

/**
 * istatement - is an interface that describes common functionality for all
 * concrete native implementations for a statement class
 */
struct istatement
{
    virtual ~istatement() { };
    virtual iresult_set* execute(const std::string& sql) = 0;
    virtual iresult_set* execute() = 0;
    virtual bool cancel() = 0;
    virtual bool cancel_all() = 0;
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
    statement(statement&& stmt) : stmt_impl(std::move(stmt.stmt_impl))
    {
    }

    statement& operator=(statement&& stmt)
    {
        if (this != &stmt)
            stmt_impl = std::move(stmt.stmt_impl);
        return *this;
    }

    result_set execute()
    {
        return result_set(stmt_impl->execute());
    }

    result_set execute(const std::string& sql)
    {
        return result_set(stmt_impl->execute(sql));
    }

    bool cancel()
    {
        return stmt_impl->cancel();
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

#endif	// STATEMENT_HPP

