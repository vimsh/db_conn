/*
 * File:   driver.hpp
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

#ifndef DRIVER_HPP
#define	DRIVER_HPP

#include <memory>
#include <functional>
#include <map>
#include "connection.hpp"

namespace vgi { namespace dbconn { namespace dbd {

/**
 * idriver - is an interface for driver classes which declares function for creating
 * a connection object
 */
struct idriver
{
    virtual dbi::connection create_connection(dbi::iconnection* iconn) = 0;
};

/**
 * driver is a singleton template class which creates a concrete driver on load()
 * and returns its instance
 */
template <typename T>
class driver : public T
{
public:
    // c++11 guaranties no threading issues
    static T& load()
    {
        static driver<T> d;
        return d;
    }

protected:
    virtual dbi::connection create_connection(dbi::iconnection* iconn)
    {
        return dbi::connection(iconn);
    }

private:
    driver() {}
    driver(const driver&);
    driver(driver&&);
    driver& operator=(const driver&);
    driver& operator=(driver&&);
    driver** operator&();
}; // driver


}}} // namespace vgi::dbconn::dbd

#endif	// DRIVER_HPP

