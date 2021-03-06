//Copyright (c) 2014-2016 OKCoin
//Author : Chenzs
//2014/04/06

#ifndef OKCOIN_LOG_H
#define OKCOIN_LOG_H


#define OKCOIN_LOG
#define _MYSQL_DB_   //写到SQL数据库

#ifdef _MYSQL_DB_
#define LOG2DB 			1
#else
#define LOG2DB 			0 	//写到文件
#endif 


#include <stdint.h>
#include <string>

#if LOG2DB
/*
  Include directly the different
  headers from cppconn/ and mysql_driver.h + mysql_util.h
  (and mysql_connection.h). This will reduce your build time!
*/
  // c++
//#include "mysql_conn_lib/include/mysql_driver.h"

#include "mysql_connection.h"
//  #include "mysql_driver.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

/*
extern  "C"{
#include "mysql.h"
}*/
	
#else

/*
 Log to file
 */
#include <boost/algorithm/string/case_conv.hpp> // for to_lower()
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp> 
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/foreach.hpp>
#define strprintf tfm::format
#define OKCoinLogPrintf(...) OKCoinLogPrint(__VA_ARGS__)


int OKCoinLogPrintStr(const std::string &str);

#define MAKE_OKCOIN_LOG_FUNC(n)                                        \
    template<TINYFORMAT_ARGTYPES(n)>                                          \
    static inline int OKCoinLogPrint(const char* format, TINYFORMAT_VARARGS(n))  \
    {                                                                         \
        return OKCoinLogPrintStr(tfm::format(format, TINYFORMAT_PASSARGS(n))); \
    }                                                                         \
    
TINYFORMAT_FOREACH_ARGNUM(MAKE_OKCOIN_LOG_FUNC)
static inline int OKCoinLogPrint(const char* format)
{
    //return LogPrintStr(format);
}

#endif


enum OKCoin_EventType{
    OC_TYPE_BLOCK = 0,
    OC_TYPE_TX = 1
 } ;

enum OKCoin_Action {
    OC_ACTION_NEW = 0,
    OC_ACTION_CONFIRM = 1,
    OC_ACTION_ORPHANE = -1
 } ;



bool OKCoin_Log_init();
bool OKCoin_Log_deInit();

/**
* 2014/07/11 chenzs
* type -- block:0 tx:1  
*/
int OKCoin_Log_Event(unsigned int type, unsigned int action , std::string hash, std::string fromip);

//剔除孤立数据
bool OKCoin_Log_EarseOrphaneBlk(std::string blkHash);
bool OKCoin_Log_EarseOrphaneTx(std::string txHash);

#endif
