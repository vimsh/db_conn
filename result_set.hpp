/*
 * File:   result_set.hpp
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

#ifndef RESULT_SET_HPP
#define RESULT_SET_HPP

#include "utilities.hpp"

namespace vgi { namespace dbconn { namespace dbi {

class statement;

/**
 * iresult_set - is an interface that describes common functionality for all
 * concrete native implementations for a result_set class
 */
struct iresult_set
{
    // c++11 uses 'move' for return of std::string and std::vector and thus efficient
    virtual ~iresult_set() { }
    virtual bool has_data() = 0;
    virtual bool more_results() = 0;
    virtual size_t row_count() const = 0;
    virtual size_t rows_affected() const = 0;
    virtual size_t column_count() const = 0;
    virtual bool next() = 0;
    virtual bool prev() = 0;
    virtual bool first() = 0;
    virtual bool last() = 0;
    virtual std::string column_name(size_t col_idx) = 0;
    virtual int column_index(const std::string& col_name) = 0;
    virtual bool is_null(size_t col_idx) = 0;
    virtual int16_t get_short(size_t col_idx) = 0;
    virtual uint16_t get_ushort(size_t col_idx) = 0;
    virtual int32_t get_int(size_t col_idx) = 0;
    virtual uint32_t get_uint(size_t col_idx) = 0;
    virtual int64_t get_long(size_t col_idx) = 0;
    virtual uint64_t get_ulong(size_t col_idx) = 0;
    virtual float get_float(size_t col_idx) = 0;
    virtual double get_double(size_t col_idx) = 0;
    virtual bool get_bool(size_t col_idx) = 0;
    virtual char get_char(size_t col_idx) = 0;
    virtual std::string get_string(size_t col_idx) = 0;
    virtual int get_date(size_t col_idx) = 0;
    virtual double get_time(size_t col_idx) = 0;
    virtual time_t get_datetime(size_t col_idx) = 0;
    virtual char16_t get_u16char(size_t col_idx) = 0;
    virtual std::u16string get_u16string(size_t col_idx) = 0;
    virtual std::vector<uint8_t> get_binary(size_t col_idx) = 0;
};


/**
 * result_set - is a class that manages native driver result_set handle.
 * result_set object cannot be instantiated directly, only via connection
 * execute() function call. result_set objects automatically delete the
 * native result_set handle they manage as soon as they themselves are destroyed,
 * or as  soon as their value changes by an assignment operation. When an
 * assignment operation takes place between two result_set objects, ownership of
 * native result_set handle is transferred, which means that the object losing
 * ownership is no longer has access to the native result_set handle.
 */
class result_set
{
public:
    /**
     * Copy constructor
     * @param rs
     */
    result_set(const result_set& rs) : rs_impl(rs.rs_impl)
    {
    }

    /**
     * Move constructor
     * @param rs
     */
    result_set(result_set&& rs) : rs_impl(std::move(rs.rs_impl))
    {
    }

    /**
     * Assignment copy operator
     * @param rs
     * @return 
     */
    result_set& operator=(const result_set& rs)
    {
        if (this != &rs)
            rs_impl = rs.rs_impl;
        return *this;
    }

    /**
     * Assignment move operator
     * @param rs
     * @return 
     */
    result_set& operator=(result_set&& rs)
    {
        if (this != &rs)
            rs_impl = std::move(rs.rs_impl);
        return *this;
    }

    /**
     * Function checks if current result set contains data.
     * Even after whole data set has been iterated through using next() the 
     * has_data() function would still return true - it is only reset after call
     * to more_results() function which will close current result set.
     * @return true if there is data in current result set, false otherwise
     */
    bool has_data()
    {
        return rs_impl->has_data();
    }

    /**
     * Function checks if there is another data set or status.
     * @return true if there is another result set, false otherwise
     */
    bool more_results()
    {
        return rs_impl->more_results();
    }

    /**
     * Function returns current row count while iterating through the result set
     * and total number of rows after data set was processed
     */
    size_t row_count() const
    {
        return rs_impl->row_count();
    }

    /**
     * Function returns number of affected rows after execution of command 
     * @return number of rows
     */
    size_t rows_affected() const
    {
        return rs_impl->rows_affected();
    }

    /**
     * Function returns number of columns of current result set or zero
     * @return number of columns or zero otherwise
     */
    size_t column_count() const
    {
        return rs_impl->column_count();
    }

    /**
     * Function returns column name by column index or throws an exception if
     * index is invalid
     * @param col_idx
     * @return column name string or exception is thrown if index is invalid
     */
    const std::string column_name(size_t col_idx)
    {
        return std::move(rs_impl->column_name(col_idx));
    }

    /**
     * Function returns column index by column name, if column name is invalid
     * then -1 is returned
     * @param col_name
     * @return index or -1 on invalid column name
     */
    int column_index(const std::string& col_name)
    {
        return rs_impl->column_index(col_name);
    }

    /**
     * Function moves iterator to the next row of the current result data set
     * @return true on success, or false if there is no more rows
     */
    bool next()
    {
        return rs_impl->next();
    }

    /**
     * Function moves iterator to the previous row of the current result data set
     * This function can only be used with scrollable cursor.
     * @return true on success, or false if there is no more rows
     */
    bool prev()
    {
        return rs_impl->prev();
    }

    /**
     * Function moves iterator to the first row of the current result data set
     * This function can only be used with scrollable cursor.
     * @return true on success, or false if there is no more rows
     */
    bool first()
    {
        return rs_impl->first();
    }

    /**
     * Function moves iterator to the last row of the current result data set
     * This function can only be used with scrollable cursor.
     * @return true on success, or false if there is no more rows
     */
    bool last()
    {
        return rs_impl->last();
    }

    /**
     * Function checks if cell data is NULL by column index or throws an exception
     * if index is invalid.
     * @param col_idx
     * @return true if cell data is NULL, false otherwise
     */
    bool is_null(size_t col_idx)
    {
        return rs_impl->is_null(col_idx);
    }

    /**
     * Function checks if cell data is NULL by column name or throws an exception
     * if index is invalid.
     * @param colname
     * @return 
     */
    bool is_null(const std::string& colname)
    {
        return rs_impl->is_null(rs_impl->column_index(colname));
    }

#define get_type_by_index(t) get_##t(size_t col_idx) { return rs_impl->get_##t(col_idx); }
#define get_type_by_name(t)  get_##t(const std::string& colname) { return rs_impl->get_##t(rs_impl->column_index(colname)); }

    int16_t get_type_by_index(short);
    int16_t get_type_by_name(short);
    uint16_t get_type_by_index(ushort);
    uint16_t get_type_by_name(ushort);
    int32_t get_type_by_index(int);
    int32_t get_type_by_name(int);
    uint32_t get_type_by_index(uint);
    uint32_t get_type_by_name(uint);
    int64_t get_type_by_index(long);
    int64_t get_type_by_name(long);
    uint64_t get_type_by_index(ulong);
    uint64_t get_type_by_name(ulong);
    float get_type_by_index(float);
    float get_type_by_name(float);
    double get_type_by_index(double);
    double get_type_by_name(double);
    bool get_type_by_index(bool);
    bool get_type_by_name(bool);
    char get_type_by_index(char);
    char get_type_by_name(char);
    std::string get_type_by_index(string);
    std::string get_type_by_name(string);
    int get_type_by_index(date);
    int get_type_by_name(date);
    double get_type_by_index(time);
    double get_type_by_name(time);
    time_t get_type_by_index(datetime);
    time_t get_type_by_name(datetime);
    char16_t get_type_by_index(u16char);
    char16_t get_type_by_name(u16char);
    std::u16string get_type_by_index(u16string);
    std::u16string get_type_by_name(u16string);
    std::vector<uint8_t> get_type_by_index(binary);
    std::vector<uint8_t> get_type_by_name(binary);


private:
    friend class statement;
    result_set(iresult_set* rs) : rs_impl(rs) { }

private:
    iresult_set* rs_impl;

}; // result_set

} } } // namespace vgi::dbconn::dbi

#endif // RESULT_SET_HPP

