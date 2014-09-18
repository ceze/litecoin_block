//Copyright (c) 2014-2016 OKCoin
//Author : Chenzs
//2014/04/06

#include "okcoin_log.h"
#include "util.h"
#include "script.h"
#include "base58.h"

//使用连接池方式
#include "mysql_connpool.h"


#if LOG2DB
#define DB_SERVER 		"rdsebnuizebnuiz.mysql.rds.aliyuncs.com:3306"
#define DB_USER	 		"coinuser"
#define DB_PASSWORD		"coin123"
#define DB_NAME			"blockchain_ltc"
#define MAX_CONNCOUNT	8


using namespace sql;
using namespace sql::mysql;
/*static sql::Connection *mysqlConn;
static sql::PreparedStatement *pstmtTx;
static sql::PreparedStatement *pstmtBlk;
static sql::PreparedStatement *pstmtOut;
static sql::PreparedStatement *pstmtUpdateTx;
static sql::PreparedStatement *pstmtEvent;
*/

static std::string db_server;
static std::string db_user;
static std::string db_password;
static std::string db_name;
static ConnPool *pConnPool;

/*
extern  "C"{
static MYSQL  mMysql;
}
*/

#else
//static boost::once_flag debugPrintInitFlag = BOOST_ONCE_INIT;
static FILE* okcoinFileout = NULL;
static boost::mutex* mutexOkcoinLog = NULL;
#define  OKCOIN_LOG_FILENAME		"okcoin_tx.log"
#endif

static bool fInited = false;





bool OKCoin_Log_init(){
	if(fInited == true){
		LogPrint("okcoin_log", "okcoin_log allready inited\n");
		return false;
	}
#if LOG2DB
	/* Create a connection */
	//load config
	db_server= GetArg("-okdbhost", DB_SERVER);
	db_user = GetArg("-okdbuser", DB_USER);
	db_password = GetArg("-okdbpassword", DB_PASSWORD);
	db_name= GetArg("-okdbname", DB_NAME);
	
	LogPrint("okcoin_log", "OKCoin_Log_init loadconfig ok_db_host = %s\n", db_server);
/*
  	Driver *driver = get_driver_instance();
  	mysqlConn = driver->connect(db_server,db_user, db_password);
  	fInited = mysqlConn? true: false;
  	assert(mysqlConn != NULL);
  	mysqlConn->setSchema(db_name);
  	LogPrint("okcoin_log", "OKCoin_Log_init log2mysqldb result = %s \n", fInited ? "success":"fails");
*/
  	pConnPool = ConnPool::GetInstance(db_server,db_user,db_password,db_name,MAX_CONNCOUNT);
  	fInited = pConnPool ? true: false;
	
	/*
extern  "C"{
	mysql_init(&mMysql);
	//connect   to   database
	mysql_real_connect(&mMysql,DB_SERVER,DB_USER,DB_PASSWORD,DB_NAME,3306,NULL,0);
	fInited = true;
}
*/
	
#else
	assert(okcoinFileout == NULL);
    assert(mutexOkcoinLog == NULL);

    boost::filesystem::path pathDebug = GetDataDir() / OKCOIN_LOG_FILENAME;
    okcoinFileout = fopen(pathDebug.string().c_str(), "a");
    if (okcoinFileout) {
    	setbuf(okcoinFileout, NULL); // unbuffered
    	fInited = true;
    }
    mutexOkcoinLog = new boost::mutex();
#endif
    return fInited;
}


bool OKCoin_Log_deInit(){
#if LOG2DB

	if(pConnPool){
		delete pConnPool;
		pConnPool = NULL;
	}

	/*
	if(mysqlConn){
		mysqlConn->close();
		delete mysqlConn;
		mysqlConn = NULL;
	}

	if(pstmtTx){
		delete pstmtTx;
		pstmtTx = NULL;
	}
			
	if(pstmtBlk){
		delete pstmtBlk;
		pstmtBlk = NULL;
	}
	if(pstmtEvent){
		delete pstmtEvent;
		pstmtEvent = NULL;
	}
	*/
		
	/*
extern  "C"{
	mysql_close(&mMysql);
    mysql_server_end();
}
*/
#else
	if(okcoinFileout != NULL)
	{
		fclose(okcoinFileout);
		okcoinFileout = NULL;
	}
	
	if(mutexOkcoinLog != NULL){
		delete mutexOkcoinLog;
		mutexOkcoinLog = NULL;
	}
#endif

	fInited = false;
	LogPrint("okcoin_log", "OKCoin_Log_deInit\n");
	return true;
}





/**
* type -- block:0 tx:1  
*/
int OKCoin_Log_Event(unsigned int type, unsigned int action,std::string hash, std::string fromip){
	assert(fInited == true);
	int ret;
#if LOG2DB
	/*
	if(pstmtEvent == NULL){
		pstmtEvent = mysqlConn->prepareStatement("CALL InsertEvent(?,?,?,?,?,?)");
	}
	*/
	sql::Connection *pConn = pConnPool->GetConnection();
	assert(pConn != NULL);
	std::auto_ptr<PreparedStatement> pstmtEvent(pConn->prepareStatement("CALL InsertLtcEvent(?,?,?,?,?,?)"));
	try{
		pstmtEvent->setInt(1, type);
		pstmtEvent->setInt(2, action);
		pstmtEvent->setString(3, hash);
		pstmtEvent->setString(4, fromip);
		pstmtEvent->setInt(5, 0);
		pstmtEvent->setDateTime(6,DateTimeStrFormat("%Y-%m-%d %H:%M:%S", GetTime()));
		ret = pstmtEvent->executeUpdate();
		pstmtEvent->close();
	}catch(sql::SQLException &e){
		LogPrint("okcoin_log", "okcoin_log Insert LTC Event type=%d err %s \n", type, e.what());
	}
	pConnPool->ReleaseConnection(pConn);

#else
	ret = OKCoinLogPrint("action:%d, type:%d block:%s ip:%s rt:%lu\n", action, type, hash.data(), fromip.data(), GetTime());
#endif
	LogPrint("okcoin_log", "okcoin_log Insert Event type=%d result= %s \n", type, ret);
	return ret;
}



#if !LOG2DB
int OKCoinLogPrintStr(const std::string &str)
{
	int ret = 0; // Returns total number of characters written
	
    if (okcoinFileout == NULL)
        return ret;

    boost::mutex::scoped_lock scoped_lock(*mutexOkcoinLog);
    ret += fprintf(okcoinFileout, "%s ", DateTimeStrFormat("%Y-%m-%d %H:%M:%S", GetTime()).c_str());
    ret = fwrite(str.data(), 1, str.size(), okcoinFileout);
    return ret;
}
#endif

