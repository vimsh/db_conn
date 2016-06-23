#include "sybase_driver.hpp"

#include <locale>
//#include <codecvt>
#include <iomanip>
using namespace std;
using namespace vgi::dbconn::dbi;
using namespace vgi::dbconn::dbd;

int main(int argc, char** argv)
{
    /************************
     * simple usage
     ************************/
    try
    {
        cout << endl << "============= Simple example =============" << endl;
        connection conn = driver<sybase::driver>::load().get_connection("DB_SYB1", "sa", "sybase");
        if (conn.connect())
        {
            cout << "connected...\n";
            statement stmt = conn.get_statement();

            stmt.execute("create table tempdb..test ( \
                          id int not null, \
                          txt1 char(25) not null, \
                          txt2 varchar(10) not null, \
                          txt3 text null, \
                          txt4 text null, \
                          bool bit not null, \
                          flag1 char(1) not null, \
                          flag2 varchar(1) not null, \
                          short1 tinyint not null, \
                          short2 smallint not null, \
                          long1 decimal not null, \
                          long2 numeric(18, 0) not null, \
                          float1 real not null, \
                          float2 float(15) not null, \
                          double1 float(16) not null, \
                          double2 double precision not null, \
                          double3 numeric(18, 8) not null, \
                          date1 date not null, \
                          date2 datetime not null, \
                          date3 smalldatetime not null, \
                          date4 bigdatetime not null, \
                          datetime1 date not null, \
                          datetime2 datetime not null, \
                          datetime3 smalldatetime not null, \
                          datetime4 bigdatetime not null, \
                          time1 time not null, \
                          time2 bigtime not null, \
                          time3 datetime not null, \
                          time4 smalldatetime not null, \
                          time5 bigdatetime not null, \
                          u16str unichar(20) not null, \
                          u16char unichar(1) not null, \
                          primary key(id) )");

            stmt.execute("insert into tempdb..test values (\
                          1, \
                          'text1', \
                          'text2', \
                          'text3', \
                          null, \
                          0, \
                          'Y', \
                          'N', \
                          1, \
                          2, \
                          167000000, \
                          167890000, \
                          1.45, \
                          2.56, \
                          12345.123456, \
                          12345.123456, \
                          12345.123456, \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          getdate(), \
                          '\u041F\u0441\u0438\u0445', \
                          '\u0414')");

            result_set rs = stmt.execute("select * from tempdb..test");
            
            time_t tm;
            while (rs.next())
            {
                cout << "-------------- data by index ----------------\n";
                cout << rs.column_name(0) << ": >" << rs.get_int(0) << "<\n";
                cout << rs.column_name(1) << ": >" << rs.get_string(1) << "<\n";
                cout << rs.column_name(2) << ": >" << rs.get_string(2) << "<\n";
                cout << rs.column_name(3) << ": >" << (rs.is_null(3) ? "NULL" : rs.get_string(4)) << "<\n";
                cout << rs.column_name(4) << ": >" << (rs.is_null(4) ? "NULL" : rs.get_string(5)) << "<\n";
                cout << rs.column_name(5) << ": >" << rs.get_bool(5) << "<\n";
                cout << rs.column_name(6) << ": >" << rs.get_char(6) << "<\n";
                cout << rs.column_name(7) << ": >" << rs.get_char(7) << "<\n";
                cout << rs.column_name(8) << ": >" << rs.get_short(8) << "<\n";
                cout << rs.column_name(9) << ": >" << rs.get_short(9) << "<\n";
                cout << rs.column_name(10) << ": >" << rs.get_long(10) << "<\n";
                cout << rs.column_name(11) << ": >" << rs.get_long(11) << "<\n";
                cout << rs.column_name(12) << ": >" << rs.get_float(12) << "<\n";
                cout << rs.column_name(13) << ": >" << rs.get_float(13) << "<\n";
                cout << rs.column_name(14) << ": >" << setprecision(12) << rs.get_double(14) << "<\n";
                cout << rs.column_name(15) << ": >" << setprecision(12) << rs.get_double(15) << "<\n";
                cout << rs.column_name(16) << ": >" << setprecision(12) << rs.get_double(16) << "<\n";
                cout << rs.column_name(17) << ": >" << rs.get_date(17) << "<\n";
                cout << rs.column_name(18) << ": >" << rs.get_date(18) << "<\n";
                cout << rs.column_name(19) << ": >" << rs.get_date(19) << "<\n";
                cout << rs.column_name(20) << ": >" << rs.get_date(20) << "<\n";
                cout << rs.column_name(21) << ": >" << ctime(&(tm = rs.get_datetime(21)));
                cout << rs.column_name(22) << ": >" << ctime(&(tm = rs.get_datetime(22)));
                cout << rs.column_name(23) << ": >" << ctime(&(tm = rs.get_datetime(23)));
                cout << rs.column_name(24) << ": >" << ctime(&(tm = rs.get_datetime(24)));
                cout << rs.column_name(25) << ": >" << rs.get_time(25) << "<\n";
                cout << rs.column_name(26) << ": >" << rs.get_time(26) << "<\n";
                cout << rs.column_name(27) << ": >" << rs.get_time(27) << "<\n";
                cout << rs.column_name(28) << ": >" << rs.get_time(28) << "<\n";
                cout << rs.column_name(29) << ": >" << rs.get_time(29) << "<\n";
                //wstring_convert<codecvt_utf8<char16_t>, char16_t> cv;
                //cout << rs.column_name(30) << ": >" << cv.to_bytes(rs.get_unistring(30)) << "<\n";
                cout << rs.column_name(30) << ": >" << rs.get_unistring(30).data() << "<\n";
                cout << rs.column_name(31) << ": >" << rs.get_unichar(31) << "<\n";
                cout << "-------------- data by name ----------------\n";
                cout << "column id: >" << rs.get_int("id") << "<\n";
                cout << "column txt1: >" << rs.get_string("txt1") << "<\n";
                cout << "column txt2: >" << rs.get_string("txt2") << "<\n";
                cout << "column txt3: >" << (rs.is_null("txt3") ? "NULL" : rs.get_string("txt3")) << "<\n";
                cout << "column txt4: >" << (rs.is_null("txt4") ? "NULL" : rs.get_string("txt4")) << "<\n";
                cout << "column bool: >" << rs.get_bool("bool") << "<\n";
                cout << "column flag1: >" << rs.get_char("flag1") << "<\n";
                cout << "column flag2: >" << rs.get_char("flag2") << "<\n";
                cout << "column short1: >" << rs.get_short("short1") << "<\n";
                cout << "column short2: >" << rs.get_short("short2") << "<\n";
                cout << "column long1: >" << rs.get_long("long1") << "<\n";
                cout << "column long2: >" << rs.get_long("long2") << "<\n";
                cout << "column float1: >" << rs.get_float("float1") << "<\n";
                cout << "column float2: >" << rs.get_float("float2") << "<\n";
                cout << "column double1: >" << setprecision(12) << rs.get_double("double1") << "<\n";
                cout << "column double2: >" << setprecision(12) << rs.get_double("double2") << "<\n";
                cout << "column double3: >" << setprecision(12) << rs.get_double("double3") << "<\n";
                cout << "column date1: >" << rs.get_date("date1") << "<\n";
                cout << "column date2: >" << rs.get_date("date1") << "<\n";
                cout << "column date3: >" << rs.get_date("date1") << "<\n";
                cout << "column date4: >" << rs.get_date("date1") << "<\n";
                cout << "column datetime1: >" << ctime(&(tm = rs.get_datetime("datetime1")));
                cout << "column datetime2: >" << ctime(&(tm = rs.get_datetime("datetime2")));
                cout << "column datetime3: >" << ctime(&(tm = rs.get_datetime("datetime3")));
                cout << "column datetime4: >" << ctime(&(tm = rs.get_datetime("datetime4")));
                cout << "column time1: >" << rs.get_time("time1") << "<\n";
                cout << "column time2: >" << rs.get_time("time2") << "<\n";
                cout << "column time3: >" << rs.get_time("time3") << "<\n";
                cout << "column time4: >" << rs.get_time("time4") << "<\n";
                cout << "column time5: >" << rs.get_time("time5") << "<\n";
                cout << "column u16str: >" << rs.get_unistring("u16str").data() << "<\n";
                cout << "column u16char: >" << rs.get_unichar("u16char") << "<\n";
            }
            
            stmt.execute("update tempdb..test set txt4 = 'text4' where id = 1");
            
            stmt.execute("delete from tempdb..test where id = 1");

            stmt.execute("drop table tempdb..test");
        }
        else
            cout << "failed to connect!\n";
    }
    catch (const exception& e)
    {
        cout << "simple example exception: " << e.what() << endl;
    }


    /************************
     * advanced usage
     ************************/
    try
    {
        cout << endl << "============= Advanced example =============" << endl;

        array<char, 256> version_string = {'\0'};
        long version = 0;
        string my_context_info = "A: 1";
        string my_conn_info = "B: 2";
        
        /*
         * Initialize sybase driver, get driver information, and set custom properties
         */
        sybase::driver& sybdriver = driver<sybase::driver>::load().
            // debug works only if compiled with debug version of sybase libraries
//            debug(sybase::debug_flag::ERROR | sybase::debug_flag::NETWORK).
//            debug_file("test.log").
//            debug_protocol_file("proto.log").
            // get version
            version(version).
            // set application name
            app_name("test.conn2").
            // set context maximum connections
            max_connections(1).
            // set connection timeout - 60 seconds
            timeout(60).
            // set keepalive (default is true)
            keepalive(true).
            // set keepalive (default is true)
            tcp_nodelay(true).
            // set custom configuration file
            config_file("/opt/sybase64/interfaces").
            // set custom locale
            locale("en_US").
            // set custom user data struct pointer - can be used to set/get custom settings
            userdata(my_context_info).
            // set custom library callback
            cs_msg_callback([](sybase::Context* context, sybase::ClientMessage* msg)
            {
                cout << __FUNCTION__ << ": CS Library message: Severity - " << CS_SEVERITY(msg->msgnumber) <<
                        " (" << sybase::driver::decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                        "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                        ", number -" << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << endl;
                if (msg->osstringlen > 0)
                    cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << endl;
                return (CS_SUCCEED);
            }).
            // set custom client callback
            ct_msg_callback([](sybase::Context* context, sybase::Connection* conn, sybase::ClientMessage* msg)
            {
                cout << __FUNCTION__ << ": Open Client Message: Severity - " << CS_SEVERITY(msg->msgnumber) <<
                        " (" << sybase::driver::decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                        "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                        ", number - " << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << endl;
                if (msg->osstringlen > 0)
                    cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << endl;
                if (nullptr != context)
                {
                    string* my_context_info = nullptr;
                    if (CS_SUCCEED == cs_config(context, CS_GET, CS_USERDATA, (CS_VOID*)&my_context_info, sizeof(my_context_info), nullptr))
                        cout << "my_context_info: " << my_context_info << endl;
                }
                return CS_SUCCEED;
            }).
            // set custom server reply callback
            srv_msg_callback([](sybase::Context* context, sybase::Connection* conn, sybase::ServerMessage* msg)
            {
                cout << __FUNCTION__ << ": Server message: " << (msg->svrnlen > 0 ? string("Server '").append(msg->svrname).append("': ") : "")
                        << (msg->proclen > 0 ? string("Procedure '").append(msg->proc).append("': ") : "") << "Severity - " << msg->severity
                        << ", state - " << msg->state << ", origin - " << msg->line << ", number - " << msg->msgnumber << ", message - " << msg->text << endl;
                if (nullptr != conn)
                {
                    string* my_conn_info = nullptr;
                    if (CS_SUCCEED == ct_con_props(conn, CS_GET, CS_USERDATA, (CS_VOID*)&my_conn_info, sizeof(my_conn_info), nullptr))
                        cout << "my_conn_info: " << *my_conn_info << endl;
                }
                return CS_SUCCEED;
            }).
            // use low level function to set/get any other desired configuration for cs/ct
            config(sybase::action::GET, sybase::cfg_type::CT_LIB, CS_VER_STRING, version_string.data(), version_string.size());

            
        /*
         * Get connection
         */
        connection conn = sybdriver.get_connection("DB_DEVSYB101", "brass_ro", "brass_ro");


        /*
         * Print information from the driver
         */
        cout << endl << "Client-Library’s true version string: " << version_string.data();
        cout << endl << "The version of Client-Library in use by this context: " << version;
        string* my_cust_context_data;
        // get the context user data that was set before
        sybdriver.userdata(my_cust_context_data);
        cout << endl << "My context custom data structure: " << *my_cust_context_data;


        /*
         * Set custom connection properties
         */
        static_cast<sybase::connection&>(conn).
            // set custom user data struct pointer - can be used to set/get custom settings
            userdata(my_conn_info).
            // use low level function to set/get any other desired configuration for cs/ct
            props(sybase::action::SET, CS_DIAG_TIMEOUT, &sybase::TRUE);


        /*
         * Print information from the connection
         */
        string* my_cust_conn_data;
        // get the connection user data that was set before
        static_cast<sybase::connection&>(conn).userdata(my_cust_conn_data);
        cout << endl << "My connection custom data structure: " << *my_cust_conn_data << endl;
        
        
        /*
         * Open connection and run some tests
         */
        if (conn.connect())
        {
            cout << "connected...\n\n";
            
            statement stmt = conn.get_statement();
            // By default, columns in Adaptive Server Enterprise default to NOT NULL, whereas in Sybase IQ the default setting is NULL
            // To allow NULL values we explicitly state so for the column
            result_set rs = stmt.execute("create table tempdb..test ( id int, txt varchar(10) NULL, dt numeric(18, 8) NULL, primary key(id) )");
            cout << "--- created table ---\n\n";
            
            /********************************************************************
             * Use multiple insert statement to populate table.
             * Some statements are invalid.
             * 
             * Note that Sybase handles errors differently depending on type of error
             */
            
            /*
             * Sybase will fail execution of whole chain of sql statements
             * since table does not exist
             */
            try
            {
                rs = stmt.execute( "insert into tempdb..test (id, txt) values (1, 'hello1') \
                                    insert into tempdb..test (id, txt) values (1, 'hello1') \
                                    insert into tempdb..test (id, txt) values (2, 'hello2') \
                                    insert into tempdb..bogus (id, txt) values (7, 'hello2') \
                                    insert into tempdb..test (id, txt) values (3, 'hello2') \
                                    insert into tempdb..test (id, txt) values (4, 'hello3') ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- insert data - example 1 ---\n\n";
            
            
            /*
             * Sybase will fail only execution of sql statements that breaks
             * unique indexing
             */
            try
            {
                rs = stmt.execute( "insert into tempdb..test (id, txt) values (1, 'hello1') \
                                    insert into tempdb..test (id, txt) values (1, 'hello1') \
                                    insert into tempdb..test (id, txt) values (2, 'hello2') \
                                    insert into tempdb..test (id, txt) values (3, 'hello2') \
                                    insert into tempdb..test (id, txt) values (4, 'hello3') \
                                    insert into tempdb..test (id, txt) values (4, 'hello3') \
                                    insert into tempdb..test (id, txt) values (5, 'hello5') ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- insert data - example 2 ---\n\n";
            
            
            /********************************************************************
             * Use multiple update statement to populate table.
             * Some statements are invalid.
             */
            
            /*
             * Sybase will fail execution of whole chain of sql statements
             * since data type is wrong for the column
             */
            try
            {
                rs = stmt.execute( "update tempdb..test set txt = 'boom' where id = 5 \
                                    update tempdb..test set txt = 'test2' where id = 6 \
                                    update tempdb..test set txt = 3 where id = 1 ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- update data - example 1 ---\n\n";
            
            /*
             * Sybase will update all matching records
             */
            try
            {
                rs = stmt.execute( "update tempdb..test set txt = 'boom' where id = 5 \
                                    update tempdb..test set txt = 'test2' where id = 6 ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- update data - example 2 ---\n\n";
            
            
            /********************************************************************
             * Use multiple select statement to retrieve data from table.
             * Some statements are invalid.
             * 
             * Note that Sybase handles errors differently depending on type of error
             */
            
            
            /*
             * Here Sybase will fail execution of whole chain of sql statements
             * as table doesn't exist
             */
            try
            {
                rs = stmt.execute( "select id from tempdb..test where txt = 'hello1' \
                                    select id, txt from tempdb..test where txt = 'hello2' \
                                    select id from tempdb..test where txt = 'hello4' \
                                    select id, txt from tempdb..bogus where txt = 'aa' \
                                    select id, txt from tempdb..test where txt = 'hello3' ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "has data = " << rs.has_data() << "\n";
            cout << "more results = " << rs.more_results() << "\n";
            cout << "--- select data - example 1 ---\n\n";
            
            
            /********************************************************************
             * Here Sybase will fail only execution of sql statements that has
             * specified invalid data type for column in where clause
             * It doesn't break the whole chain though unlike insert/update
             */
            try
            {
                rs = stmt.execute( "select id from tempdb..test where txt = 'hello1' \
                                    select id, txt from tempdb..test where txt = 'hello2' \
                                    select id from tempdb..test where txt = 'hello4' \
                                    select id, txt from tempdb..test where txt = 1 \
                                    select id, txt from tempdb..test where txt = 'hello3' ");
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
            cout << "--- select data - example 2 ---\n\n";
            
            
            
            /********************************************************************
             * Using compute in select
             */
            try
            {
                rs = stmt.execute( "select id, txt, dt from tempdb..test order by id, txt compute count(txt) by id, txt ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            for (int i = 1; rs.more_results(); ++i)
            {
                try
                {
                    cout << "Result " << i << ": data set (" << rs.column_count() << " columns):\n";
                    if (rs.has_data())
                    {
                        while (rs.next())
                        {
                            cout << "\trow " << rs.row_count() << ":\n";
                            cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                            if (rs.column_count() > 1)
                                cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
                        }
                    }
                    cout << "---------\n";
                }
                catch (const exception& e)
                {
                    cout << "select " << i << ": exception: " << e.what() << "\n";
                    cout << "---------\n";
                }
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- select data - example 3 ---\n\n";
            
            
            /********************************************************************
             * Use prepared SQL statements for faster execution of repeated statements
             */
            
            
            /*
             * Prepared SQL statements for select
             */
            stmt.prepare("select id, txt from tempdb..test where id = ? or txt = ?");
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
            cout << "--- prepared stmt select 1 ---\n\n";
            
            stmt.prepare("select min(id) from tempdb..test where txt = ?");
            cout << "--------------\n";
            stmt.set_string(0, "hello2");
            rs = stmt.execute();
            if (rs.has_data())
            {
                while (rs.next())
                {
                    cout << "\trow " << rs.row_count() << ":\n";
                    cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                }
                cout << "done\n";
            }
            else
                cout << "no data\n";
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- prepared stmt select 2 ---\n\n";
            

            /*
             * Prepared SQL statements for update
             * 
             * NOTE!!! weirdly the Sybase does not return row count for prepared
             * update SQL statements
             */
            stmt.prepare("update tempdb..test set txt = ?, dt = ? where id = ?");
            for (int i = 4; i < 6; ++i)
            {
                cout << "--------------\n";
                stmt.set_null(0);
                stmt.set_ldouble(1, 12345.123456);
                stmt.set_int(2, i);
                rs = stmt.execute();
                cout << "rows affected = " << rs.rows_affected() << "\n";
            }
            cout << "--- prepared stmt update - example 1 ---\n\n";
            
            stmt.prepare("update tempdb..test set txt = ? where id = ?");
            for (int i = 5; i < 7; ++i)
            {
                cout << "--------------\n";
                string txt("aaqqq");
                txt.append(std::to_string(i));
                stmt.set_string(0, txt);
                stmt.set_int(1, i);
                rs = stmt.execute();
                cout << "rows affected = " << rs.rows_affected() << "\n";
            }
            cout << "--- prepared stmt update - example 2 ---\n\n";
            
            /*
             * Prepared SQL statements for delete
             * 
             * NOTE!!! weirdly the Sybase does not return row count for prepared
             * delete SQL statements
             */
            stmt.prepare("delete from tempdb..test where id = ?");
            cout << "--------------\n";
            stmt.set_int(0, 4);
            rs = stmt.execute();
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "--- prepared stmt delete - example ---\n\n";
            
            
            
            /********************************************************************
             * Use delete SQL statement.
             */
            rs = stmt.execute("delete from tempdb..test where id = 1 or id = 2");
            cout << "delete : rows affected = " << rs.rows_affected() << "\n";
            cout << "--- deleted data ---\n\n";
            
            
            
            
            /********************************************************************
             * Use drop table SQL statement.
             */
            rs = stmt.execute("drop table tempdb..test");
            cout << "drop : rows affected = " << rs.rows_affected() << "\n";
            cout << "--- dropped table ---\n\n";
        
            
            
            /********************************************************************
             * Use autocommit OFF mode
             */
            stmt.execute("create table tempdb..test (id int, txt varchar(10) NULL, primary key(id) )");
            conn.autocommit(false);
            cout << "autocommit is OFF\n";
            cout << "insert and commit 1 row\n";
            stmt.execute("insert into tempdb..test values (1, 'test1')");
            conn.commit();
            cout << "insert and rollback 2 rows\n";
            stmt.execute("insert into tempdb..test values (2, 'test2')");
            stmt.execute("insert into tempdb..test values (3, 'test3')");
            conn.rollback();
            conn.autocommit(true);
            cout << "autocommit is ON\n";
            cout << "check row count to make sure rollback worked\n";
            rs = stmt.execute( "select count(*) from tempdb..test");
            rs.next();
            cout << "\tTable has >" << rs.get_int(0) << "< rows\n";
            stmt.execute("drop table tempdb..test");
        }
        else
            cout << "failed to connect!\n";
    }
    catch (const exception& e)
    {
        cout << "advanced example exception: " << e.what() << endl;
    }

    return 0;
}
