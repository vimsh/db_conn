/*
 * File:   connection.hpp
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

#ifndef CONNECTION_HPP
#define	CONNECTION_HPP

#include <memory>
#include "utilities.hpp"
#include "driver.hpp"
#include "statement.hpp"

// forward declaration
namespace vgi { namespace dbconn { namespace dbd {

    template <typename T> class driver;

} } } // namespace vgi::dbconn::dbd

namespace vgi { namespace dbconn { namespace dbi {

/**
 * iconnection - is an interface that describes common functionality for all
 * concrete native implementations for a connection class
 */
struct iconnection
{
    virtual ~iconnection() { };
    virtual bool connect() = 0;
    virtual void disconnect() = 0;
    virtual void commit() = 0;
    virtual void rollback() = 0;
    virtual bool connected() const = 0;
    virtual bool alive() const = 0;
    virtual istatement* get_statement(iconnection&) = 0;
};


/**
 * connection - is a class that manages native driver connection handle.
 * connection object cannot be instantiated directly, only via driver
 * get_connection() function call. connection objects automatically delete the
 * native connection handle they manage as soon as they themselves are destroyed,
 * or as  soon as their value changes by an assignment operation. When an
 * assignment operation takes place between two connection objects, ownership of
 * native connection handle is transferred, which means that the object losing
 * ownership is no longer has access to the native connection handle.
 */
class connection {

public:
    connection(connection&& conn) : conn_impl(std::move(conn.conn_impl))
    {
    }

    connection& operator=(connection&& conn)
    {
        if (this != &conn)
            conn_impl = std::move(conn.conn_impl);
        return *this;
    }

    ~connection()
    {
        disconnect();
    }

    bool connect()
    {
        return conn_impl->connect();
    }

    void disconnect()
    {
        conn_impl->disconnect();
    }

    void commit()
    {
        conn_impl->commit();
    }

    void rollback()
    {
        conn_impl->rollback();
    }

    bool connected() const
    {
        return conn_impl->connected();
    }

    bool alive() const
    {
        return conn_impl->alive();
    }

    statement get_statement()
    {
        return statement(conn_impl->get_statement(*(conn_impl.get())));
    }

    template <typename T>
    T& get_native_connection()
    {
        return dynamic_cast<T&>(*conn_impl);
    }

private:
    template <typename T> friend class dbd::driver;
    friend class statement;

    connection(iconnection* conn) : conn_impl(conn) { }
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

private:
    std::unique_ptr<iconnection> conn_impl;

}; // connection

} } } // namespace vgi::dbconn::dbi

#endif	// CONNECTION_HPP

