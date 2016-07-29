#include "sybase_driver.hpp"

#include <locale>
//#include <codecvt>
#include <iomanip>
using namespace std;
using namespace vgi::dbconn::dbi;
using namespace vgi::dbconn::dbd;

constexpr auto DBNAME = "DBSYB1";
constexpr auto DBUSER = "sa";
constexpr auto DBPASS = "sybase";

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
        connection conn = driver<sybase::driver>::load().get_connection(DBNAME, DBUSER, DBPASS);
        if (conn.connect())
        {
            cout << "===== done...\n\n";
            statement stmt = conn.get_statement();
            
            cout << "===== switching database\n";
            stmt.execute("use tempdb");
            cout << "===== done...\n\n";
            
            try
            {
                stmt.execute("if object_id('tempdb..test') is not null drop table test");
            }
            catch (const exception&)
            {
                // ignore exception if table doesn't exist
            }
            cout << "===== creating table\n";
            stmt.execute("create table test (                "
                         "id int not null,                   "
                         "txt1 char(25) not null,            "
                         "txt2 varchar(10) not null,         "
#ifndef CS_VERSION_150 // for using old sybase 12.5 client with sybase 12 or 15 server
                         "txt3 varchar(256) null,            "
                         "txt4 varchar(256) null,            "
#else
                         "txt3 text null,                    "
                         "txt4 text null,                    "
#endif
                         "bool bit not null,                 "
                         "flag1 char(1) not null,            "
                         "flag2 varchar(1) not null,         "
                         "short1 tinyint not null,           "
                         "short2 smallint not null,          "
                         "long1 decimal not null,            "
                         "long2 numeric not null,            "
                         "float1 real not null,              "
                         "float2 float(15) not null,         "
                         "double1 float(16) not null,        "
#ifndef CS_VERSION_150
                         "double2 float(18) not null, "
#else
                         "double2 double precision not null, "
#endif
                         "double3 numeric(18, 8) not null,   "
                         "date1 date not null,               "
                         "date2 datetime not null,           "
                         "date3 smalldatetime not null,      "
#ifndef CS_VERSION_150
                         "date4 datetime not null,        "
#else
                         "date4 bigdatetime not null,        "
#endif
            
                         "datetime1 date not null,           "
                         "datetime2 datetime not null,       "
                         "datetime3 smalldatetime not null,  "
            
#ifndef CS_VERSION_150
                         "datetime4 datetime not null,    "
#else
                         "datetime4 bigdatetime not null,    "
#endif      
                         "time1 time not null,               "
#ifndef CS_VERSION_150
                         "time2 time not null,            "
#else
                         "time2 bigtime not null,            "
#endif
                         "time3 datetime not null,           "
                         "time4 smalldatetime not null,      "
#ifndef CS_VERSION_150
                         "time5 datetime not null,        "
#else
                         "time5 bigdatetime not null,        "
#endif
                         "u16str1 univarchar(20) not null,   "
                         "u16str2 unichar(20) not null,      "
                         "u16char unichar(1) not null,       "
            
                         "nstr1 nchar(20) not null,          "
                         "nstr2 nvarchar(20) not null,       "
                         "nchar nchar(1) not null,           "
            
#ifndef CS_VERSION_150
                         "img binary(250),                   "
#else
                         "img image,                         "
#endif
                         "bin1 binary(25),                   "
                         "bin2 varbinary(25),                "
                         "mon1 smallmoney,                   "
                         "mon2 money,                        "
                         "primary key(id) )                  ");
            cout << "===== done...\n\n";

            cout << "===== inserting row into the table\n";
            stmt.execute("insert into test values (\
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
                          '\u041F\u0441\u0438\u0445', \
                          '\u0414', \
                          '\u041F\u0441\u0438\u0445', \
                          '\u041F\u0441\u0438\u0445', \
                          '\u0414', \
                          0x0000008300000000000100000000013c, \
                          0x01234, \
                          0x0123456789, \
                          214748.3647, \
                          122337203685477.58)");
            cout << "===== done...\n\n";

            cout << "===== selecting data from the table\n";
            result_set rs = stmt.execute("select * from test");
            
            int date;
            time_t datetime;
            double time;
            std::u16string unistr;
            std::string nstr;
            std::vector<uint8_t> imgvec;
            std::vector<uint8_t> binvec1;
            std::vector<uint8_t> binvec2;
            
            time_t tm;
            while (rs.next())
            {
                cout << "-------------- data by index\n";
                cout << rs.column_name(0) << ": >" << rs.get_int(0) << "<\n";
                cout << rs.column_name(1) << ": >" << rs.get_string(1) << "<\n";
                cout << rs.column_name(2) << ": >" << rs.get_string(2) << "<\n";
                cout << rs.column_name(3) << ": >" << (rs.is_null(3) ? "NULL" : rs.get_string(3)) << "<\n";
                cout << rs.column_name(4) << ": >" << (rs.is_null(4) ? "NULL" : rs.get_string(4)) << "<\n";
                cout << rs.column_name(5) << ": >" << rs.get_bool(5) << "<\n";
                cout << rs.column_name(6) << ": >" << rs.get_char(6) << "<\n";
                cout << rs.column_name(7) << ": >" << rs.get_char(7) << "<\n";
                cout << rs.column_name(8) << ": >" << rs.get_short(8) << "<\n";
                cout << rs.column_name(9) << ": >" << rs.get_short(9) << "<\n";
                cout << rs.column_name(10) << ": >" << rs.get_long(10) << "<\n";
                cout << rs.column_name(11) << ": >" << rs.get_long(11) << "<\n";
                cout << rs.column_name(12) << ": >" << rs.get_float(12) << "<\n";
                cout << rs.column_name(13) << ": >" << rs.get_float(13) << "<\n";
                cout << rs.column_name(14) << ": >" << rs.get_double(14) << "<\n";
                cout << rs.column_name(15) << ": >" << rs.get_double(15) << "<\n";
                cout << rs.column_name(16) << ": >" << rs.get_double(16) << "<\n";
                cout << rs.column_name(17) << ": >" << rs.get_date(17) << "<\n"; date = rs.get_date(17);
                cout << rs.column_name(18) << ": >" << rs.get_date(18) << "<\n";
                cout << rs.column_name(19) << ": >" << rs.get_date(19) << "<\n";
                cout << rs.column_name(20) << ": >" << rs.get_date(20) << "<\n";
                cout << rs.column_name(21) << ": >" << ctime(&(tm = rs.get_datetime(21)));
                cout << rs.column_name(22) << ": >" << ctime(&(tm = rs.get_datetime(22))); datetime = rs.get_datetime(22);
                cout << rs.column_name(23) << ": >" << ctime(&(tm = rs.get_datetime(23)));
                cout << rs.column_name(24) << ": >" << ctime(&(tm = rs.get_datetime(24)));
                cout << rs.column_name(25) << ": >" << rs.get_time(25) << "<\n"; time = rs.get_time(25);
                cout << rs.column_name(26) << ": >" << rs.get_time(26) << "<\n";
                cout << rs.column_name(27) << ": >" << rs.get_time(27) << "<\n";
                cout << rs.column_name(28) << ": >" << rs.get_time(28) << "<\n";
                cout << rs.column_name(29) << ": >" << rs.get_time(29) << "<\n";
                //wstring_convert<codecvt_utf8<char16_t>, char16_t> cv;
                //cout << rs.column_name(30) << ": >" << cv.to_bytes(rs.get_unistring(30)) << "<\n";
                //cout << rs.column_name(32) << ": >" << cv.to_bytes(rs.get_unistring(31)) << "<\n";
                //cout << rs.column_name(31) << ": >" << cv.to_bytes(rs.get_unichar(32)) << "<\n";
                cout << rs.column_name(30) << ": >" << rs.get_unistring(30).data() << "<\n"; // this is utf-16  
                unistr = rs.get_unistring(30);
                cout << rs.column_name(31) << ": >" << rs.get_unistring(31).data() << "<\n"; // this is utf-16
                cout << rs.column_name(32) << ": >" << rs.get_unichar(32) << "<\n"; // this is utf-16
                cout << rs.column_name(33) << ": >" << rs.get_string(33) << "<\n"; // this is utf-8
                nstr = rs.get_string(33);
                cout << rs.column_name(34) << ": >" << rs.get_string(34) << "<\n"; // this is utf-8
                cout << rs.column_name(35) << ": >" << rs.get_char(35) << "<\n"; // this is utf-8
                auto idata1 = rs.get_binary(36);
                imgvec.insert(imgvec.begin(), idata1.begin(), idata1.end());
                cout << rs.column_name(36) << ": >";
                cout.setf(ios_base::hex, ios::basefield);
                for (uint16_t i : idata1)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                auto bdata1 = rs.get_binary(37);
                binvec1.insert(binvec1.begin(), bdata1.begin(), bdata1.end());
                cout << rs.column_name(37) << ": >";
                cout.setf(ios_base::hex, ios::basefield);
                for (uint16_t i : bdata1)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                auto bbdata1 = rs.get_binary(38);
                binvec2.insert(binvec2.begin(), bbdata1.begin(), bbdata1.end());
                cout << rs.column_name(38) << ": >";
                cout.setf(ios_base::hex, ios::basefield);
                for (uint16_t i : bbdata1)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                cout << rs.column_name(39) << ": >" << rs.get_double(39) << "<\n";
                cout << rs.column_name(40) << ": >" << rs.get_double(40) << "<\n";
                cout << "-------------- data by name\n";
                cout << "column id: >" << rs.get_int("id") << "<\n";
                cout << "column txt1: >" << rs.get_string("txt1") << "<\n";
                cout << "column txt2: >" << rs.get_string("txt2") << "<\n";
                cout << "column txt3: >" << (rs.is_null("txt3") ? "NULL" : rs.get_string("txt3")) << "<\n";
                cout << "column txt4: >" << (rs.is_null("txt4") ? "NULL" : rs.get_string("txt4")) << "<\n";
                cout << "column bool: >" << rs.get_bool("bool") << "<\n";
                cout << "column flag1: >" << rs.get_char("flag1") << "<\n";
                cout << "column flag2: >" << rs.get_char("flag2") << "<\n";
                cout << "column short1: >" << rs.get_long("short1") << "<\n";
                cout << "column short2: >" << rs.get_long("short2") << "<\n";
                cout << "column long1: >" << rs.get_long("long1") << "<\n";
                cout << "column long2: >" << rs.get_long("long2") << "<\n";
                cout << "column float1: >" << rs.get_float("float1") << "<\n";
                cout << "column float2: >" << rs.get_float("float2") << "<\n";
                cout << "column double1: >" << rs.get_double("double1") << "<\n";
                cout << "column double2: >" << rs.get_double("double2") << "<\n";
                cout << "column double3: >" << rs.get_double("double3") << "<\n";
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
                cout << "column u16str1: >" << rs.get_unistring("u16str1").data() << "<\n"; // this is utf-16
                cout << "column u16str2: >" << rs.get_unistring("u16str2").data() << "<\n"; // this is utf-16
                cout << "column u16char: >" << rs.get_unichar("u16char") << "<\n"; // this is utf-16
                cout << "column nstr1: >" << rs.get_string("nstr1") << "<\n"; // this is utf-8
                cout << "column nstr2: >" << rs.get_string("nstr2") << "<\n"; // this is utf-8
                cout << "column nchar: >" << rs.get_char("nchar") << "<\n"; // this is utf-8
                cout << setfill('0') << setw(2) << hex;
                auto idata2 = rs.get_binary("img");
                cout << "column img: >";
                cout.setf(ios_base::hex, ios::basefield);
                for (uint16_t i : idata2)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                auto bdata2 = rs.get_binary("bin1");
                cout << "column bin: >";
                cout.setf(ios_base::hex, ios::basefield);
                for (uint16_t i : bdata2)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                auto bbdata2 = rs.get_binary("bin2");
                cout << "column bin: >";
                cout.setf(ios_base::hex, ios::basefield);
                for (uint16_t i : bbdata2)
                    cout << setfill('0') << setw(2) << i;
                cout.setf(ios_base::dec, ios::basefield);
                cout << "<\n";
                cout << "column mon1: >" << rs.get_double("mon1") << "<\n";
                cout << "column mon2: >" << rs.get_double("mon2") << "<\n";
            }
            cout << "===== done...\n\n";

            cout << "===== testing prepared statement (stored proc would be the same) data types binding\n";
            stmt.prepare("insert into test values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.set_int(0, 2);
            stmt.set_string(1, "text1");
            stmt.set_string(2, "text2");
            stmt.set_string(3, "text3");
            stmt.set_null(4);
            stmt.set_bool(5, false);
            stmt.set_char(6, 'Y');
            stmt.set_char(7, 'N');
            stmt.set_short(8, 1);
            stmt.set_short(9, 2);
            stmt.set_long(10, 167000000);
            stmt.set_long(11, 167890000);
            stmt.set_float(12, 1.45);
            stmt.set_float(13, 2.56);
            stmt.set_double(14, 12345.123456);
            stmt.set_double(15, 12345.123456);
            stmt.set_double(16, 12345.123456);
            stmt.set_date(17, date);
            stmt.set_date(18, date);
            stmt.set_date(19, date);
            stmt.set_date(20, date);
            stmt.set_datetime(21, datetime);
            stmt.set_datetime(22, datetime);
            stmt.set_datetime(23, datetime);
            stmt.set_datetime(24, datetime);
            stmt.set_time(25, time);
            stmt.set_time(26, time);
            stmt.set_time(27, time);
            stmt.set_time(28, time);
            stmt.set_time(29, time);
            stmt.set_unistring(30, unistr);
            stmt.set_unistring(31, unistr);
            stmt.set_unichar(32, unistr[0]);
            stmt.set_string(33, nstr);
            stmt.set_string(34, nstr);
            stmt.set_char(35, nstr[0]);
            stmt.set_binary(36, imgvec);
            stmt.set_binary(37, binvec1);
            stmt.set_binary(38, binvec2);
            stmt.set_double(39, 214748.3647);
            stmt.set_double(40, 122337203685477.58);
            stmt.execute();
            cout << "===== done...\n\n";
            
            cout << "===== updating row in the table\n";
            stmt.execute("update test set txt4 = 'text4' where id = 1");
            cout << "===== done...\n\n";
            
            cout << "===== deleting from the table\n";
            stmt.execute("delete from test where id = 1");
            cout << "===== done...\n\n";

            cout << "===== dropping the table\n";
            stmt.execute("drop table test");
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

        array<char, 256> version_string = {'\0'};
        long version = 0;
        string my_context_info = "A: 1";
        string my_conn_info = "B: 2";
        
        cout << "===== connecting to database server using custom settings\n";
        /*
         * Initialize Sybase driver, get driver information, and set custom properties
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
                return CS_SUCCEED;
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
        connection conn = sybdriver.get_connection(DBNAME, DBUSER, DBPASS);

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
            cout << "===== done...\n\n";
            
            statement stmt = conn.get_statement();
            
            cout << "===== switching database\n";
            stmt.execute("use tempdb");
            cout << "===== done...\n\n";
            
            cout << "===== creating table\n";
            // By default, columns in Adaptive Server Enterprise default to NOT NULL, whereas in Sybase IQ the default setting is NULL
            // To allow NULL values we explicitly state so for the column
            result_set rs = stmt.execute("create table test ( id int, txt varchar(10) NULL, dt numeric(18, 8) NULL, primary key(id) )");
            cout << "===== done...\n\n";
            
            /********************************************************************
             * Use multiple insert statement to populate table.
             * Some statements are invalid.
             * 
             * Note that Sybase handles errors differently depending on type of error
             */
            
            cout << "===== using multiple statements to insert rows into the table - all will fail on invalid table name in one of the statements\n";
            /*
             * Sybase will fail execution of whole chain of sql statements
             * since table does not exist
             */
            try
            {
                rs = stmt.execute( "insert into test (id, txt) values (1, 'hello1') \
                                    insert into test (id, txt) values (1, 'hello1') \
                                    insert into test (id, txt) values (2, 'hello2') \
                                    insert into bogus (id, txt) values (7, 'hello2') \
                                    insert into test (id, txt) values (3, 'hello2') \
                                    insert into test (id, txt) values (4, 'hello3') ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using multiple statements to insert rows into the table - two will fail on broken unique index\n";
            /*
             * Sybase will fail only execution of sql statements that breaks
             * unique indexing
             */
            try
            {
                rs = stmt.execute( "insert into test (id, txt) values (1, 'hello1') \
                                    insert into test (id, txt) values (1, 'hello1') \
                                    insert into test (id, txt) values (2, 'hello2') \
                                    insert into test (id, txt) values (3, 'hello2') \
                                    insert into test (id, txt) values (4, 'hello3') \
                                    insert into test (id, txt) values (4, 'hello3') \
                                    insert into test (id, txt) values (5, 'hello5') ");
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
            
            cout << "===== using multiple statements to update rows in the table - all will fail on invalid data type for the column in one of the statements\n";
            /*
             * Sybase will fail execution of whole chain of sql statements
             * since data type is wrong for the column
             */
            try
            {
                rs = stmt.execute( "update test set txt = 'boom' where id = 5 \
                                    update test set txt = 'test2' where id = 6 \
                                    update test set txt = 3 where id = 1 ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            cout << "===== using multiple statements to update rows in the table\n";
            /*
             * Sybase will update all matching records
             */
            try
            {
                rs = stmt.execute( "update test set txt = 'boom' where id = 5 \
                                    update test set txt = 'test2' where id = 6 ");
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
             * 
             * Note that Sybase handles errors differently depending on type of error
             */
            
            
            cout << "===== using multiple statements to select data from the table - all will fail on invalid table name in one of the statements\n";
            /*
             * Here Sybase will fail execution of whole chain of sql statements
             * as table doesn't exist
             */
            try
            {
                rs = stmt.execute( "select id from test where txt = 'hello1' \
                                    select id, txt from test where txt = 'hello2' \
                                    select id from test where txt = 'hello4' \
                                    select id, txt from bogus where txt = 'aa' \
                                    select id, txt from test where txt = 'hello3' ");
            }
            catch (const exception& e)
            {
                cout << e.what() << "\n";
            }
            cout << "has data = " << rs.has_data() << "\n";
            cout << "more results = " << rs.more_results() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using multiple statements to select data from the table - one will fail on invalid data type for the column (unlike isert/update which fail all)\n";
            /********************************************************************
             * Here Sybase will fail only execution of sql statements that has
             * specified invalid data type for column in where clause
             * It doesn't break the whole chain though unlike insert/update
             */
            try
            {
                rs = stmt.execute( "select id from test where txt = 'hello1' \
                                    select id, txt from test where txt = 'hello2' \
                                    select id from test where txt = 'hello4' \
                                    select id, txt from test where txt = 1 \
                                    select id, txt from test where txt = 'hello3' ");
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
            
            
            cout << "===== using compute select statements\n";
            /********************************************************************
             * Using compute in select
             */
            try
            {
                rs = stmt.execute( "select id, txt, dt from test order by id, txt compute count(txt) by id, txt ");
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
            cout << "===== done...\n\n";
            
            
            /********************************************************************
             * Use prepared SQL statements for faster execution of repeated statements
             */
            
            
            cout << "===== using prepared select statements - repeated execution setting new values\n";
            /*
             * Prepared SQL statements for select
             */
            stmt.prepare("select id, txt from test where id = ? or txt = ?");
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

            
            cout << "===== using prepared update statements - repeated execution setting new values (no affected row count is available)\n";
            /*
             * Prepared SQL statements for update
             * 
             * NOTE!!! weirdly the Sybase does not return affected row count for prepared
             * update SQL statements
             */
            stmt.prepare("update test set txt = ?, dt = ? where id = ?");
            for (int i = 4; i < 6; ++i)
            {
                cout << "--------------\n";
                stmt.set_null(0);
                stmt.set_double(1, 12345.123456);
                stmt.set_int(2, i);
                rs = stmt.execute();
                cout << "rows affected = " << rs.rows_affected() << "\n";
            }
            cout << "===== done...\n\n";      
            
            cout << "===== using prepared delete statements - repeated execution setting new values (no affected row count is available)\n";
            /*
             * Prepared SQL statements for delete
             * 
             * NOTE!!! weirdly the Sybase does not return row count for prepared
             * delete SQL statements
             */
            stmt.prepare("delete from test where id = ?");
            cout << "--------------\n";
            stmt.set_int(0, 4);
            rs = stmt.execute();
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            /********************************************************************
             * Use SQL stored procedures
             */
            try
            {
                stmt.execute("drop procedure test_proc");
            }
            catch (const exception&)
            {
                // ignore exception if procedure doesn't exist
            }
            cout << "===== creating stored procedure\n";
            stmt.execute("CREATE PROCEDURE test_proc @id INT, @error VARCHAR(128) output, @status VARCHAR(20) output AS \
                          BEGIN                                                             \
                              DECLARE @ret int                                              \
                              SET @error = NULL                                             \
                              SET @ret = 0                                                  \
                              IF @id IS NULL                                                \
                                  BEGIN                                                     \
                                      SET @error = 'You need to pass in an id number'       \
                                      SET @status = 'Ooops'                                 \
                                      SET @id = -2222222                                    \
                                      SET @ret = 1                                          \
                                  END                                                       \
                              ELSE IF @id = 5                                               \
                                  BEGIN                                                     \
                                      SET @error = 'ID 5 is not allowed'                    \
                                      SET @status = 'Exception'                             \
                                      SET @id = -2222222                                    \
                                      SET @ret = 2                                          \
                                      RAISERROR 90001 @error                                \
                                  END                                                       \
                              ELSE                                                          \
                                      SET @status = 'OK'                                    \
                              PRINT @status                                                 \
                              SELECT txt FROM test WHERE id = @id                           \
                              RETURN @ret                                                   \
                          END");
            cout << "===== done...\n\n";
            
            
            cout << "===== using stored procedure\n";
            stmt.call("test_proc");
            stmt.set_int(0, 3);
            cout << "--------------\n";
            rs = stmt.execute();
            while (rs.next())
            {
                cout << "\trow " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? "NULL" : rs.get_string(0)) << "<\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "stored proc return = " << stmt.proc_retval() << "\n";
            cout << "@error: >" << (rs.is_null(0) ? "NULL" : rs.get_string("@error")) << "<\n";
            cout << "@status: >" << (rs.is_null(1) ? "NULL" : rs.get_string("@status")) << "<\n";
            
            stmt.set_null(0);
            cout << "--------------\n";
            rs = stmt.execute();
            while (rs.next())
            {
                cout << "\trow " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? "NULL" : rs.get_string(0)) << "<\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "stored proc return = " << stmt.proc_retval() << "\n";
            cout << "@error: >" << (rs.is_null(0) ? "NULL" : rs.get_string("@error")) << "<\n";
            cout << "@status: >" << (rs.is_null(1) ? "NULL" : rs.get_string("@status")) << "<\n";
            
            stmt.set_int(0, 5);
            cout << "--------------\n";
            rs = stmt.execute();
            while (rs.next())
            {
                cout << "\trow " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? "NULL" : rs.get_string(0)) << "<\n";
            }
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "stored proc return = " << stmt.proc_retval() << "\n";
            cout << "@error: >" << (rs.is_null(0) ? "NULL" : rs.get_string("@error")) << "<\n";
            cout << "@status: >" << (rs.is_null(1) ? "NULL" : rs.get_string("@status")) << "<\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using delete with number of affected rows\n";
            /********************************************************************
             * Use delete SQL statement.
             */
            rs = stmt.execute("delete from test where id = 1 or id = 2");
            cout << "delete : rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using drop with number of affected rows\n";
            /********************************************************************
             * Use drop table SQL statement.
             */
            rs = stmt.execute("drop table test");
            cout << "drop : rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            
            cout << "===== using explicit commit\n";
            /********************************************************************
             * Use autocommit OFF mode
             */
            stmt.execute("create table test (id int, txt varchar(10) NULL, primary key(id) )");
            conn.autocommit(false);
            cout << "autocommit is OFF\n";
            cout << "insert 1 row\n";
            stmt.execute("insert into test values (1, 'test1')");
            cout << "run commit\n";
            conn.commit();
            cout << "insert 2 rows\n";
            stmt.execute("insert into test values (2, 'test2')");
            stmt.execute("insert into test values (3, 'test3')");
            cout << "run rollback\n";
            conn.rollback();
            conn.autocommit(true);
            cout << "autocommit is ON\n";
            cout << "check row count to make sure rollback worked\n";
            rs = stmt.execute( "select count(*) from test");
            rs.next();
            cout << "\tTable has >" << rs.get_int(0) << "< rows\n";
            cout << "===== done...\n\n";
            
            
            /********************************************************************
             * Use cursors for SELECT statements
             */
            stmt.execute("insert into test values (2, 'test2')");
            stmt.execute("insert into test values (3, 'test3')");
            stmt.execute("insert into test values (4, 'test4')");
            stmt.execute("insert into test values (5, 'test5')");
            
            cout << "===== using cursor for select statements - read only cursor (note that rows affected is not returned by sybase)\n";
            rs = stmt.execute("select id, txt from test", true);
            while (rs.next())
            {
                cout << "\trow " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
            }
            cout << "done\n";
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
            
            cout << "===== using cursor for select statements - scrollable cursor (note that rows affected is not returned by sybase)\n";
            rs = stmt.execute("select id, txt from test", true, true);
            while (rs.next())
            {
                cout << "row " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
            }
            
            cout << "-------------- rewinding the cursor to the start\n";
            rs.first();
            do
            {
                cout << "row " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
            }
            while (rs.next());
            
            cout << "-------------- iterating from end to start\n";
            while (rs.prev())
            {
                cout << "row " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
            }
            
            cout << "-------------- rewinding the cursor to the end - resets row count and starts increasing the count\n";
            rs.last();
            do
            {
                cout << "row " << rs.row_count() << ":\n";
                cout << "\t" << rs.column_name(0) << ": >" << (rs.is_null(0) ? -1 : rs.get_int(0)) << "<\n";
                cout << "\t" << rs.column_name(1) << ": >" << (rs.is_null(1) ? "NULL" : rs.get_string(1)) << "<\n";
            }
            while (rs.prev());
            cout << "rows affected = " << rs.rows_affected() << "\n";
            cout << "===== done...\n\n";
                        
            stmt.execute("drop table test");
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
