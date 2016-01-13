/*
 * File:   utilities.hpp
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

#ifndef UTILITIES_HPP
#define	UTILITIES_HPP

#include <functional>
#include <iostream>
#include <type_traits>
#include <mutex>
#include <atomic>

namespace std {
    template<typename T>
    ostream& operator<<(typename enable_if<is_enum<T>::value, ostream>::type& stream, const T& e)
    {
        return stream << static_cast<typename underlying_type<T>::type>(e);
    }
} // namespace std

namespace vgi { namespace dbconn { namespace utils {

    template<typename T>
    constexpr typename std::underlying_type<T>::type base_type(T t) { return typename std::underlying_type<T>::type(t); }

    class spin_lock
    {
    public:
        void lock()
        {
            while(lck.test_and_set(std::memory_order_acquire))
            {}
        }

        void unlock()
        {
            lck.clear(std::memory_order_release);
        }

    private:
        std::atomic_flag lck = ATOMIC_FLAG_INIT;
    };

} } } // namepsace vgi::dbconn::utils

#endif	// UTILITIES_HPP

