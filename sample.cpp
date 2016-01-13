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
        connection conn = driver<sybase::driver>::load().get_connection("DB_DEVSYB101", "sa", "ascasc");
        if (conn.connect())
        {
            cout << "connected...\n";
            statement stmt = conn.get_statement();

            stmt.execute("create table tempdb..vimsh ( \
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

            stmt.execute("insert into tempdb..vimsh values (\
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

            result_set rs = stmt.execute("select * from tempdb..vimsh");
            time_t tm;
            while (rs.next())
            {
                cout << "------------------------------\n";
                cout << "column 1: >" << rs.get_int(0) << "<\n";
                cout << "column 2: >" << rs.get_string(1) << "<\n";
                cout << "column 3: >" << rs.get_string(2) << "<\n";
                cout << "column 4: >" << (rs.is_null(3) ? "NULL" : rs.get_string(4)) << "<\n";
                cout << "column 5: >" << (rs.is_null(4) ? "NULL" : rs.get_string(5)) << "<\n";
                cout << "column 6: >" << rs.get_bool(5) << "<\n";
                cout << "column 7: >" << rs.get_char(6) << "<\n";
                cout << "column 8: >" << rs.get_char(7) << "<\n";
                cout << "column 9: >" << rs.get_short(8) << "<\n";
                cout << "column 10: >" << rs.get_short(9) << "<\n";
                cout << "column 11: >" << rs.get_long(10) << "<\n";
                cout << "column 12: >" << rs.get_long(11) << "<\n";
                cout << "column 13: >" << rs.get_float(12) << "<\n";
                cout << "column 14: >" << rs.get_float(13) << "<\n";
                cout << "column 15: >" << setprecision(12) << rs.get_double(14) << "<\n";
                cout << "column 16: >" << setprecision(12) << rs.get_double(15) << "<\n";
                cout << "column 17: >" << setprecision(12) << rs.get_double(16) << "<\n";
                cout << "column 18: >" << rs.get_date(17) << "<\n";
                cout << "column 19: >" << rs.get_date(18) << "<\n";
                cout << "column 20: >" << rs.get_date(19) << "<\n";
                cout << "column 21: >" << rs.get_date(20) << "<\n";
                cout << "column 22: >" << ctime(&(tm = rs.get_datetime(21)));
                cout << "column 23: >" << ctime(&(tm = rs.get_datetime(22)));
                cout << "column 24: >" << ctime(&(tm = rs.get_datetime(23)));
                cout << "column 25: >" << ctime(&(tm = rs.get_datetime(24)));
                cout << "column 26: >" << rs.get_time(25) << "<\n";
                cout << "column 27: >" << rs.get_time(26) << "<\n";
                cout << "column 28: >" << rs.get_time(27) << "<\n";
                cout << "column 29: >" << rs.get_time(28) << "<\n";
                cout << "column 30: >" << rs.get_time(29) << "<\n";
                //wstring_convert<codecvt_utf8<char16_t>, char16_t> cv;
                //cout << "column 31: >" << cv.to_bytes(rs.get_unistring(30)) << "<\n";
                cout << "column 31: >" << rs.get_unistring(30).data() << "<\n";
                cout << "column 32: >" << rs.get_unichar(31) << "<\n";
                cout << "------------------------------\n";
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

            stmt.execute("delete from tempdb..vimsh where id = 1");

            stmt.execute("drop table tempdb..vimsh");
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

        std::array<char, 256> version_string = {'\0'};
        long version = 0;
        std::string iface_file = "/opt/sybase64/interfaces";

        connection conn = driver<sybase::driver>::load().
// debug works only if compiled with debug version of sybase libraries
//            debug(sybase::debug_flag::ERROR | sybase::debug_flag::NETWORK).
//            debug_file("test.log").
//            debug_protocol_file("proto.log").
            config(sybase::action::GET, sybase::cfg_type::CT_LIB, CS_VER_STRING, version_string.data(), version_string.size()).
            config(sybase::action::GET, sybase::cfg_type::CS_LIB, CS_VERSION, &version).
            cs_msg_callback([](CS_CONTEXT* context, CS_CLIENTMSG* msg)
            {
                cout << __FUNCTION__ << ": CS Library message: Severity - " << CS_SEVERITY(msg->msgnumber) <<
                        " (" << sybase::driver::decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                        "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                        ", number -" << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << endl;
                if (msg->osstringlen > 0)
                    cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << endl;
                return (CS_SUCCEED);
            }).
            ct_msg_callback([](CS_CONTEXT* context, CS_CONNECTION* conn, CS_CLIENTMSG* msg)
            {
                cout << __FUNCTION__ << ": Open Client Message: Severity - " << CS_SEVERITY(msg->msgnumber) <<
                        " (" << sybase::driver::decode_severity(CS_SEVERITY(msg->msgnumber)) <<
                        "), layer - " << CS_LAYER(msg->msgnumber) << ", origin - " << CS_ORIGIN(msg->msgnumber) <<
                        ", number - " << CS_NUMBER(msg->msgnumber) << ", message - " << msg->msgstring << endl;
                if (msg->osstringlen > 0)
                    cout << __FUNCTION__ << ": Operating System Message: " << msg->osstring << endl;
                return CS_SUCCEED;
            }).
            srv_msg_callback([](CS_CONTEXT* context, CS_CONNECTION* conn, CS_SERVERMSG* msg)
            {
                cout << __FUNCTION__ << ": Server message: " << (msg->svrnlen > 0 ? string("Server '").append(msg->svrname).append("': ") : "")
                        << (msg->proclen > 0 ? string("Procedure '").append(msg->proc).append("': ") : "") << "Severity - " << msg->severity
                        << ", state - " << msg->state << ", origin - " << msg->line << ", number - " << msg->msgnumber << ", message - " << msg->text << endl;
                return CS_SUCCEED;
            }).
            get_connection("DB_DEVSYB101", "brass_ro", "brass_ro", "test.conn2");

        conn.get_native_connection<sybase::connection>().
            props(sybase::action::SET, CS_CONFIG_FILE, const_cast<char*>(iface_file.c_str()), CS_NULLTERM).
            props(sybase::action::SET, CS_DIAG_TIMEOUT, &sybase::TRUE);


        cout << endl << "Client Library Version: " << version_string.data();
        cout << endl << "Open Client Library Version: " << version << endl;

        if (conn.connect())
        {
            cout << "connected...\n";
            statement stmt = conn.get_statement();
            result_set rs = stmt.execute("create table tempdb..vimsh ( id int, txt varchar(10), primary key(id) )");
            cout << "created table!\n";
            rs = stmt.execute( "insert into tempdb..vimsh values (1, 'hello') \
                                insert into tempdb..vimsh values (1, 'hello') \
                                insert into tempdb..vimsh values (2, 'hello') ");
            bool more = true;
            for (int i = 1; more; ++i)
            {
                cout << "insert " << i << ": rows affected = " << rs.row_count() << "\n";
                try
                {
                    more = rs.more_results();
                }
                catch (const exception& e)
                {
                    cout << "insert " << i + 1 << ": exception: " << e.what() << "\n";
                }
            }
            rs = stmt.execute("delete from tempdb..vimsh where id = 1 or id = 2");
            cout << "delete: rows affected = " << rs.row_count() << "\n";
            cout << "dropping table!\n";
            rs = stmt.execute("drop table tempdb..vimsh");
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
