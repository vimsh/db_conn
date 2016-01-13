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
#define	RESULT_SET_HPP

namespace vgi { namespace dbconn { namespace dbi {

class statement;

/**
 * iresult_set - is an interface that describes common functionality for all
 * concrete native implementations for a result_set class
 */
struct iresult_set
{
    // c++11 uses 'move' for return of std::string and std::vector and thus efficient
    virtual ~iresult_set() { };
    virtual bool has_data() = 0;
    virtual bool more_results() = 0;
    virtual size_t row_count() const = 0;
    virtual size_t column_count() const = 0;
    virtual bool next() = 0;
    virtual std::string column_name(size_t index) = 0;
    virtual int column_index(const std::string& col_name) = 0;
    virtual bool is_null(unsigned int colidx) = 0;
    virtual int16_t get_short(unsigned int colidx) = 0;
    virtual uint16_t get_ushort(unsigned int colidx) = 0;
    virtual int32_t get_int(unsigned int colidx) = 0;
    virtual uint32_t get_uint(unsigned int colidx) = 0;
    virtual int64_t get_long(unsigned int colidx) = 0;
    virtual uint64_t get_ulong(unsigned int colidx) = 0;
    virtual float get_float(unsigned int colidx) = 0;
    virtual double get_double(unsigned int colidx) = 0;
    virtual long double get_ldouble(unsigned int colidx) = 0;
    virtual bool get_bool(unsigned int colidx) = 0;
    virtual char get_char(unsigned int colidx) = 0;
    virtual std::string get_string(unsigned int colidx) = 0;
    virtual int get_date(unsigned int colidx) = 0;
    virtual double get_time(unsigned int colidx) = 0;
    virtual time_t get_datetime(unsigned int colidx) = 0;
    virtual char16_t get_unichar(unsigned int colidx) = 0;
    virtual std::u16string get_unistring(unsigned int colidx) = 0;
    virtual std::vector<uint8_t> get_image(unsigned int colidx) = 0;
    // TODO: add xml, binary, money, lob
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
    result_set(const result_set& rs) : rs_impl(rs.rs_impl)
    {
    }

    result_set(result_set&& rs) : rs_impl(std::move(rs.rs_impl))
    {
    }

    result_set& operator=(const result_set& rs)
    {
        if (this != &rs)
            rs_impl = rs.rs_impl;
        return *this;
    }

    result_set& operator=(result_set&& rs)
    {
        if (this != &rs)
            rs_impl = std::move(rs.rs_impl);
        return *this;
    }

    bool has_data()
    {
        return rs_impl->has_data();
    }

    bool more_results()
    {
        return rs_impl->more_results();
    }

    size_t row_count() const
    {
        return rs_impl->row_count();
    }

    size_t column_count() const
    {
        return rs_impl->column_count();
    }

    const std::string column_name(size_t index)
    {
        return rs_impl->column_name(index);
    }

    int column_index(const std::string& col_name)
    {
        return rs_impl->column_index(col_name);
    }

    bool next()
    {
        return rs_impl->next();
    }

    bool is_null(unsigned int colidx)
    {
        return rs_impl->is_null(colidx);
    }

    bool is_null(const std::string& colname)
    {
        return rs_impl->is_null(rs_impl->column_index(colname));
    }

#define get_type_by_index(t)	get_##t(unsigned int colidx) { return rs_impl->get_##t(colidx); }
#define get_type_by_name(t)	    get_##t(const std::string& colname) { return rs_impl->get_##t(rs_impl->column_index(colname)); }

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
    long double get_type_by_index(ldouble);
    long double get_type_by_name(ldouble);
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
    char16_t get_type_by_index(unichar);
    char16_t get_type_by_name(unichar);
    std::u16string get_type_by_index(unistring);
    std::u16string get_type_by_name(unistring);
    std::vector<uint8_t> get_type_by_index(image);
    std::vector<uint8_t> get_type_by_name(image);


private:
    friend class statement;
    result_set(iresult_set* rs) : rs_impl(rs) { }

private:
    iresult_set* rs_impl;

}; // result_set

} } } // namespace vgi::dbconn::dbi

#endif	// RESULT_SET_HPP

