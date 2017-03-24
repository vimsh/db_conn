\mainpage db_conn


Header files based C++ database connection API based on KISS principle

### The why?

I had a hard time finding a free open source C++ Sybase driver that would be lightweight with no dependencies, easy to add and use, having a generic extensible interface in case database would be switched later on. Since then I also added SQLite driver.


### Thread safety:

The driver functions are thread-safe, all other objects (connection, statement, result_set) are not.

It's up to the users to decide whether to use (and what kind) any synchronization means or use a different design, eg:

* use a single connection per thread
* create a connection pool
* create a separate database connection thread with its own connection


### Currently supported databases:

* Sybase ASE (ASA was not tested) - see sybase_example.cpp
* SQLite - see sqlite_example.cpp


### Development state:

The code has not been extensively tested, thus there could be some bugs.

Especial concerns would be the large data type (text, blob, unitext) and utf support.


### Usage example (same for all drivers for supported features - see sample files)

@code
    
    #include "sybase_driver.hpp"
    
    using namespace std;
    using namespace vgi::dbconn::dbi;
    using namespace vgi::dbconn::dbd;
    
    int main(int argc, char** argv)
    {
        try
        {
            connection conn = driver<sybase::driver>::load().get_connection("DBSYB1", "sa", "");
            if (conn.connect())
            {
                statement stmt = conn.get_statement();
                
                // change database
                stmt.execute("use tempdb");
                
                stmt.execute("if object_id('tempdb..test') is not null drop table test");
                
                // create
                stmt.execute("create table test (id int, txt varchar(10) null, num numeric(18, 8) null, primary key(id))");
                
                // insert
                stmt.execute("insert into test (id, txt) values (1, 'txt1') \
                              insert into test (id, txt) values (2, 'txt2')");
                
                // update
                stmt.execute("update test set txt = 'test1' where id = 1 \
                              update test set txt = 'test2' where id = 2");
                
                // select
                result_set rs = stmt.execute("select * from test");
                while (rs.next())
                {
                    cout << rs.column_name(0) << ": " << rs.get_int(0) << endl;
                    cout << rs.column_name(1) << ": " << rs.get_string(1) << endl;
                    cout << "column id: " << rs.get_int("id") << endl;
                    cout << "column txt1: " << rs.get_string("txt") << endl;
                }
                
                // delete
                stmt.prepare("delete from test where id = 2");
                rs = stmt.execute();
                cout << "rows affected = " << rs.rows_affected() << endl;
                
                // prepared statement
                stmt.prepare("insert into test values (?, ?)");
                stmt.set_int(0, 2);
                stmt.set_string(1, "test2");
                stmt.execute();
                
                // cursor
                rs = stmt.execute("select id, txt from test", true);
                while (rs.next())
                {
                    cout << rs.column_name(0) << ": " << (rs.is_null(0) ? -1 : rs.get_int(0)) << endl;
                    cout << rs.column_name(1) << ": " << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << endl;
                }
                
                // scrollable cursor
                rs = stmt.execute("select id, txt from test", true, true);
                while (rs.next())
                {
                    cout << rs.column_name(0) << ": " << (rs.is_null(0) ? -1 : rs.get_int(0)) << endl;
                    cout << rs.column_name(1) << ": " << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << endl;
                }
                rs.first();
                do
                {
                    cout << rs.column_name(0) << ": " << (rs.is_null(0) ? -1 : rs.get_int(0)) << endl;
                    cout << rs.column_name(1) << ": " << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << endl;
                }
                while (rs.next());
                while (rs.prev())
                {
                    cout << rs.column_name(0) << ": " << (rs.is_null(0) ? -1 : rs.get_int(0)) << endl;
                    cout << rs.column_name(1) << ": " << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << endl;
                }
                
                // stored procedure
                stmt.execute("create procedure test_proc @id int, @error varchar(128) output AS \
                              BEGIN                                                             \
                                  DECLARE @ret int                                              \
                                  SET @error = NULL                                             \
                                  SET @ret = 0                                                  \
                                  IF @id = 0                                                    \
                                      BEGIN                                                     \
                                          SET @error = 'id must be > 0'                         \
                                          SET @ret = 1                                          \
                                      END                                                       \
                                  SELECT txt FROM test WHERE id = @id                           \
                                  RETURN @ret                                                   \
                              END");
                stmt.call("test_proc");
                stmt.set_int(0, 3);
                rs = stmt.execute();
                while (rs.next())
                    cout << rs.column_name(0) << ": " << (rs.is_null(0) ? "NULL" : rs.get_string(0)) << endl;
                cout << "rows affected = " << rs.rows_affected() << endl;
                cout << "stored proc return = " << stmt.proc_retval() << endl;
                cout << "@error: >" << (rs.is_null(0) ? "NULL" : rs.get_string("@error")) << endl;
                
                // truncate
                stmt.execute("truncate table test");
                
                // transaction
                conn.autocommit(false);
                stmt.execute("insert into test values (1, 'test1')");
                conn.commit();
                stmt.execute("insert into test values (2, 'test2')");
                stmt.execute("insert into test values (3, 'test3')");
                conn.rollback();
                conn.autocommit(true);
                rs = stmt.execute("select count(*) from test");
                rs.next();
                cout << "Table has " << rs.get_int(0) << " rows\n";
                
                // drop
                stmt.execute("drop table test");
            }
            else
                cout << "failed to connect!\n";
        }
        catch (const exception& e)
        {
            cout << "exception: " << e.what() << endl;
        }
        return 0;
    }            
    

@endcode

Limited amount of testing was done.

If someone would like to contribute please ping me.

