#include "sqlite_driver.hpp"

#include <iomanip>
using namespace std;
using namespace vgi::dbconn::dbi;
using namespace vgi::dbconn::dbd;

constexpr auto DBNAME = ":memory:";
//constexpr auto DBNAME = "DBTEST.db";

int main(int argc, char** argv)
{
    /************************
     * simple usage
     ************************/
    try
    {
        cout.precision(12);
        cout.setf(ios_base::fixed, ios::floatfield);
        
        cout << endl << "============= Simple example =============" << endl;
        cout << "===== connecting to database server\n";
        connection conn = driver<sqlite::driver>::load().get_connection(DBNAME);
        if (conn.connect())
        {
            cout << "===== done...\n\n";
            statement stmt = conn.get_statement();
            
            cout << "===== drop table if it exists\n";
            stmt.execute("drop table if exists test; ");
            cout << "===== done...\n\n";
            
            cout << "===== creating table\n";
            stmt.execute("create table test (      "
                         "id integer not null,     "
                         "txt1 text not null,      "
                         "txt2 text null,          "
                         "bool integer not null,   "
                         "flag text not null,      "
                         "short integer not null,  "
                         "long integer not null,   "
                         "float real not null,     "
                         "double real not null,    "
                         "date1 int not null,      "
                         "date2 text not null,     "
                         "date3 real not null,     "
                         "datetime1 int not null,  "
                         "datetime2 text not null, "
                         "datetime3 real not null, "
                         "time1 int not null,      "
                         "time2 text not null,     "
                         "time3 real not null,     "
                         "u16str text16 not null,  "
                         "u16char text16 not null, "
                         "bin blob,                "
                         "primary key(id) );       ");
            cout << "===== done...\n\n";

            cout << "===== inserting row into the table\n";
            stmt.execute("insert into test values (             "
                         "1,                                    "
                         "'text1',                              "
                         "null,                                 "
                         "0,                                    "
                         "'Y',                                  "
                         "2,                                    "
                         "167890000,                            "
                         "12345.123,                            "
                         "122337203685477.58,                   "
                         "CURRENT_DATE,                         "
                         "CURRENT_DATE,                         "
                         "CURRENT_DATE,                         "
                         "CURRENT_TIMESTAMP,                    "
                         "CURRENT_TIMESTAMP,                    "
                         "CURRENT_TIMESTAMP,                    "
                         "CURRENT_TIME,                         "
                         "CURRENT_TIME,                         "
                         "CURRENT_TIME,                         "
                         "'\u041F\u0441\u0438\u0445',           "
                         "'\u0414',                             "
                         "X'0000008300000000000100000000013c'); ");
            cout << "===== done...\n\n";

            cout << "===== selecting data from the table\n";
            result_set rs = stmt.execute("select * from test;");
            
            int date;
            time_t datetime;
            double time;
            std::u16string u16str;
            char16_t char16;
            char* c;
            std::vector<uint8_t> binvec;
            
            time_t tm;
            while (rs.next())
            {
                size_t i = 0;
                cout << "-------------- data by index\n";
                cout << rs.column_name(i) << ": >" << rs.get_int(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_string(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << (rs.is_null(i) ? "NULL" : rs.get_string(i)) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_bool(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_char(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_short(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_long(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_float(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_double(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_date(i) << "<\n"; date = rs.get_date(i); ++i;
                cout << rs.column_name(i) << ": >" << rs.get_date(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_date(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << ctime(&(tm = rs.get_datetime(i))); ++i;
                cout << rs.column_name(i) << ": >" << ctime(&(tm = rs.get_datetime(i))); datetime = rs.get_datetime(i); ++i;
                cout << rs.column_name(i) << ": >" << ctime(&(tm = rs.get_datetime(i))); ++i;
                cout << rs.column_name(i) << ": >" << rs.get_time(i) << "<\n"; time = rs.get_time(i); ++i;
                cout << rs.column_name(i) << ": >" << rs.get_time(i) << "<\n"; ++i;
                cout << rs.column_name(i) << ": >" << rs.get_time(i) << "<\n"; ++i;
                // Standard code conversion facets
                //wstring_convert<codecvt_utf8<char16_t>, char16_t> cv;
                //cout << rs.column_name(i) << ": >" << cv.to_bytes(rs.get_u16string(i)) << "<\n"; ++i;
                //cout << rs.column_name(i) << ": >" << cv.to_bytes(rs.get_u16char(i)) << "<\n"; ++i;
                // Or just print out each char after manual conversion
                cout.setf(ios_base::hex, ios::basefield);
                cout << rs.column_name(i) << ": >";
                u16str = rs.get_u16string(i); ++i;
                for (auto chr16 : u16str)
                {
                    // little endian
                    c = reinterpret_cast<char*>(&chr16);
                    cout << "\\u" << setfill('0') << setw(2) << hex << uppercase << (int)(*(++c));
                    cout << setfill('0') << setw(2) << hex << uppercase << (int)(*(--c));
                }
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                cout << rs.column_name(i) << ": >";
                char16 = rs.get_u16char(i); ++i;
                c = reinterpret_cast<char*>(&char16);
                // little endian
                cout << "\\u" << setfill('0') << setw(2) << hex << uppercase << (int)(*(++c));
                cout << setfill('0') << setw(2) << hex << uppercase << (int)(*(--c));
                cout << "<\n";
                // End of code conversion facets
                
                auto bdata1 = rs.get_binary(i);
                binvec.insert(binvec.begin(), bdata1.begin(), bdata1.end());
                cout << rs.column_name(i) << ": >"; ++i;
                for (uint16_t k : binvec)
                    cout << setfill('0') << setw(2) << k;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                
                cout << "-------------- data by name\n";
                cout << "column id: >" << rs.get_int("id") << "<\n";
                cout << "column txt1: >" << rs.get_string("txt1") << "<\n";
                cout << "column txt2: >" << (rs.is_null("txt2") ? "NULL" : rs.get_string("txt2")) << "<\n";
                cout << "column bool: >" << rs.get_bool("bool") << "<\n";
                cout << "column flag: >" << rs.get_char("flag") << "<\n";
                cout << "column short: >" << rs.get_long("short") << "<\n";
                cout << "column long: >" << rs.get_long("long") << "<\n";
                cout << "column float: >" << rs.get_float("float") << "<\n";
                cout << "column double: >" << rs.get_double("double") << "<\n";
                cout << "column date1: >" << rs.get_date("date1") << "<\n";
                cout << "column date2: >" << rs.get_date("date2") << "<\n";
                cout << "column date3: >" << rs.get_date("date3") << "<\n";
                cout << "column datetime1: >" << ctime(&(tm = rs.get_datetime("datetime1")));
                cout << "column datetime2: >" << ctime(&(tm = rs.get_datetime("datetime2")));
                cout << "column datetime3: >" << ctime(&(tm = rs.get_datetime("datetime3")));
                cout << "column time1: >" << rs.get_time("time1") << "<\n";
                cout << "column time2: >" << rs.get_time("time2") << "<\n";
                cout << "column time3: >" << rs.get_time("time3") << "<\n";
                cout.setf(ios_base::hex, ios::basefield);
                cout << "column u16str1: >";
                u16str = rs.get_u16string("u16str");
                for (auto chr16 : u16str)
                {
                    // little endian
                    c = reinterpret_cast<char*>(&chr16);
                    cout << "\\u" << setfill('0') << setw(2) << hex << uppercase << (int)(*(++c));
                    cout << setfill('0') << setw(2) << hex << uppercase << (int)(*(--c));
                }
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                cout << "column u16char: >";
                char16 = rs.get_u16char("u16char");
                c = reinterpret_cast<char*>(&char16);
                // little endian
                cout << "\\u" << setfill('0') << setw(2) << hex << uppercase << (int)(*(++c));
                cout << setfill('0') << setw(2) << hex << uppercase << (int)(*(--c));
                cout << "<\n";
                auto bdata2 = rs.get_binary("bin");
                cout << "column bin: >";
                for (uint16_t i : bdata2)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
            }
            cout << "===== done...\n\n";

            cout << "===== testing prepared statement (stored proc would be the same) data types binding\n";
            u16str = u"\u041F\u0441\u0438\u0445";
            size_t i = 0;
            stmt.prepare("insert into test values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
            stmt.set_int(i++, 2);
            stmt.set_string(i++, "text1");
            stmt.set_null(i++);
            stmt.set_bool(i++, false);
            stmt.set_char(i++, 'Y');
            stmt.set_short(i++, 2);
            stmt.set_long(i++, 167890000);
            stmt.set_float(i++, 12345.123);
            stmt.set_double(i++, 122337203685477.58);
            stmt.set_date(i++, date);
            stmt.set_date(i++, date);
            stmt.set_date(i++, date);
            stmt.set_datetime(i++, datetime);
            stmt.set_datetime(i++, datetime);
            stmt.set_datetime(i++, datetime);
            stmt.set_time(i++, time);
            stmt.set_time(i++, time);
            stmt.set_time(i++, time);
            stmt.set_u16string(i++, u16str);
            stmt.set_u16char(i++, u16str[0]);
            stmt.set_binary(i++, binvec);
            stmt.execute();
            cout << "===== done...\n\n";
 
            cout << "===== updating row in the table\n";
            stmt.execute("update test set txt1 = 'text_1.1' where id = 1; ");
            cout << "===== done...\n\n";
            
            cout << "===== deleting from the table\n";
            stmt.execute("delete from test where id = 1; ");
            cout << "===== done...\n\n";

            cout << "===== dropping the table\n";
            stmt.execute("drop table test; ");
            cout << "===== done...\n\n";
        }
        else
            cout << "===== failed to connect!\n";
    }
    catch (const exception& e)
    {
        cout << "===== simple example exception: " << e.what() << endl;
    }

    /************************
     * advanced usage
     ************************/
    try
    {
        cout << endl << "============= Advanced example =============" << endl;

        long version = 0;
        string verstr;
        bool verbose = true;
        
        cout << "===== connecting to database server using custom settings\n";
        /*
         * Initialize SQLite driver, get driver information, and set custom properties
         */
        sqlite::driver& sqltdriver = driver<sqlite::driver>::load().
            // get version
            version(version).
            version_string(verstr).
            // set maximum connections
            max_connections(1).
            // set custome config settings
            config(sqlite::config_flag::MEMSTATUS, 1).
            config(sqlite::config_flag::SOFT_HEAP_LIMIT, 8 * 1024 * 1024).
#ifdef SQLITE_CONFIG_LOG
            config(sqlite::config_flag::LOG, [](void* data, int errcode, const char* msg)
            {
                bool isverbose = !!data;
                if ((errcode == 0 || errcode == SQLITE_CONSTRAINT || errcode == SQLITE_SCHEMA) && isverbose)
                    cout << __FUNCTION__ << ": Error Code: " << errcode << ": Message: " << msg << endl;
                else
                    cout << __FUNCTION__ << ": Error Code: " << errcode << ": Message: " << msg << endl;
            }, (verbose ? reinterpret_cast<void*>(1) : nullptr))
#endif
            config(sqlite::config_flag::MULTITHREAD);
            
        /*
         * Print information from the driver
         */
        cout << "SQLite Library version number: " << version << endl;
        cout << "SQLite Library version string: " << verstr << endl;

        /*
         * Get connection
         */
        cout << "===== connecting to database server\n";
        connection conn = sqltdriver.get_connection(DBNAME);
        if (conn.connect())
        {
            cout << "===== done...\n\n";
        
            /*
             * Set custom connection properties
             */
            static_cast<sqlite::connection&>(conn).
                config(sqlite::db_config_flag::LOOKASIDE, nullptr, 0, 0);
            
            statement stmt = conn.get_statement();
            
            cout << "===== drop table if it exists\n";
            result_set rs = stmt.execute("drop table if exists test; ");
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            cout << "===== creating table\n";
            rs = stmt.execute("create table test (id integer not null, txt text null, date int not null, primary key(id) );");
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            /********************************************************************
             * Use multiple insert statement to populate table.
             * Some statements are invalid.
             */

            cout << "===== using multiple statements to insert rows into the table - all will fail on invalid table name in one of the statements\n";
            // If failed to prepare all statement - then whole chain fails
            try
            {
                rs = stmt.execute("insert into test  values (1, 'hello1', CURRENT_DATE); \
                                   insert into test  values (1, 'hello1', CURRENT_DATE); \
                                   insert into test  values (2, 'hello2', CURRENT_DATE); \
                                   insert into test  values (7, 'hello2', CURRENT_DATE); \
                                   insert into bogus values (3, 'hello2', CURRENT_DATE); \
                                   insert into test  values (4, 'hello3', CURRENT_DATE); ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using multiple statements to insert rows into the table - two will fail on broken unique index\n";
            // If certain statements fail during runtime - then all the other statements continue to be executed
            try
            {
                rs = stmt.execute("insert into test values (1, 'hello1', CURRENT_DATE); \
                                   insert into test values (1, 'hello1', CURRENT_DATE); \
                                   insert into test values (2, 'hello2', CURRENT_DATE); \
                                   insert into test values (3, 'hello2', CURRENT_DATE); \
                                   insert into test values (4, 'hello3', CURRENT_DATE); \
                                   insert into test values (4, 'hello3', CURRENT_DATE); \
                                   insert into test values (5, 'hello5', CURRENT_DATE); ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            
            /********************************************************************
             * Use multiple update statement to populate table.
             * Some statements are invalid.
             */
            
            cout << "===== using multiple statements to update rows in the table - all will fail on invalid table name in one of the statements\n";
            // If failed to prepare all statement (invalid table name) - then whole chain fails
            try
            {
                rs = stmt.execute("update test  set txt = 'boom'  where id = 1; \
                                   update bogus set txt = 'test2' where id = 2; \
                                   update test  set id = 3 where id = 4;      ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using multiple statements to update rows in the table - one will fail on invalid id\n";
            // If certain statements fail during runtime - then all the other statements continue to be executed
            try
            {
                rs = stmt.execute("update test set txt = 'boom'  where id = 5; \
                                   update test set txt = 'test2' where id = 6;      ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            /********************************************************************
             * Use multiple select statement to retrieve data from table.
             * Some statements are invalid.
             */
            
            cout << "===== using multiple statements to select data from the table - all will fail on invalid table name in one of the statements\n";
            // If failed to prepare all statement (invalid table name) - then whole chain fails
            try
            {
                rs = stmt.execute( "select id      from test  where txt = 'hello1'; \
                                    select id, txt from test  where txt = 'hello2'; \
                                    select id      from test  where txt = 'hello4'; \
                                    select id, txt from bogus where txt = 'aa';     \
                                    select id, txt from test  where txt = 'hello3'; ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "has data = " << rs.has_data() << "\n";
            cout << "more results = " << rs.more_results() << "\n";
            cout << "===== done...\n\n";
            
            
            
            
            cout << "===== using multiple statements to select data from the table - one will return no data\n";
            // If certain statements fail during runtime - then all the other statements continue to be executed
            try
            {
                rs = stmt.execute( "select id      from test where txt = 'hello1'; \
                                    select id, txt from test where txt = 'hello2'; \
                                    select id      from test where txt = 'hello4'; \
                                    select id, txt from test where txt = 'hello3'; ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            for (int i = 1; rs.more_results(); ++i)
            {
                try
                {
                    cout << "select " << i << ": data set (" << rs.column_count() << " columns):\n";
                    if (rs.has_data())
                    {
                        while (rs.next())
                        {
                            cout << "\trow " << rs.row_count() << ":\n";
                            cout << "\tcolumn 1: >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                            if (rs.column_count() > 1)
                                cout << "\tcolumn 2: >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
                        }
                    }
                    cout << "rows affected = " << rs.rows_affected() << "\n";
                    cout << "---------\n";
                }
                catch (const exception& e)
                {
                    cout << "select " << i << ": exception: " << e.what() << "\n";
                    cout << "---------\n";
                }
            }
            cout << "===== done...\n\n";
            
            
            
            /********************************************************************
             * Use prepared SQL statements for faster execution of repeated statements
             */
            
            
            cout << "===== using prepared select statements - repeated execution setting new values\n";
            /*
             * Prepared SQL statements for select
             */
            stmt.prepare("select id, txt from test where id = ? or txt = ?; ");
            for (int i = 0; i < 6; ++i)
            {
                cout << "--------------\n";
                stmt.set_int(0, i);
                stmt.set_string(1, "hello3");
                rs = stmt.execute();
                if (rs.has_data())
                {
                    while (rs.next())
                    {
                        cout << "\trow " << rs.row_count() << ":\n";
                        cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                        cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
                    }
                    cout << "done\n";
                }
                else
                    cout << "no data\n";
                cout << "rows affected = " << rs.rows_affected() << "\n";
            }
            cout << "===== done...\n\n";

            
            cout << "===== using prepared update statements - repeated execution setting new values\n";
            /*
             * Prepared SQL statements for update
             */
            stmt.prepare("update test set txt = ?, date = ? where id = ?; ");
            for (int i = 4; i < 6; ++i)
            {
                cout << "--------------\n";
                stmt.set_null(0);
                stmt.set_long(1, 20170101);
                stmt.set_int(2, i);
                rs = stmt.execute();
                cout << "rows affected = " << rs.rows_affected() << "\n";
            }
            cout << "===== done...\n\n";
            
            cout << "===== using prepared delete statements - repeated execution setting new values\n";
            /*
             * Prepared SQL statements for delete
             */
            stmt.prepare("delete from test where id = ?;");
            cout << "--------------\n";
            stmt.set_int(0, 4);
            rs = stmt.execute();
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            
            cout << "===== using delete with number of affected rows\n";
            /********************************************************************
             * Use delete SQL statement.
             */
            rs = stmt.execute("delete from test where id = 1 or id = 2;");
            cout << "delete : rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using drop with number of affected rows\n";
            /********************************************************************
             * Use drop table SQL statement.
             */
            rs = stmt.execute("drop table test;");
            cout << "drop : rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using explicit commit\n";
            /********************************************************************
             * Use autocommit OFF mode
             */
            stmt.execute("create table test (id int, txt text NULL, primary key(id) );");
            conn.autocommit(false);
            cout << "autocommit is OFF\n";
            cout << "insert 1 row\n";
            stmt.execute("insert into test values (1, 'test1');");
            cout << "run commit\n";
            conn.commit();
            cout << "insert 2 rows\n";
            stmt.execute("insert into test values (2, 'test2');");
            stmt.execute("insert into test values (3, 'test3');");
            cout << "run rollback\n";
            conn.rollback();
            conn.autocommit(true);
            cout << "autocommit is ON\n";
            cout << "check row count to make sure rollback worked\n";
            rs = stmt.execute( "select count(*) from test;");
            rs.next();
            cout << "\tTable has >" << rs.get_int(0) << "< rows\n";
            cout << "===== done...\n\n";
            
        }
    }
    catch (const exception& e)
    {
        cout << "advanced example exception: " << e.what() << endl;
    }

    return 0;
}

