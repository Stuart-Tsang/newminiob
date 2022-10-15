# pragma once

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <sstream>
#include "rc.h"
#include "sql/parser/parse_defs.h"

inline bool is_leap_year(int year)
{
    return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

inline bool is_date_form(const char *str)
{
    int n1, n2, n3, res=0;
    res = sscanf(str, "%d-%d-%d", &n1, &n2, &n3);
    if(res == 3)
        return true;
    return false;
}

inline RC string_to_date(const char *str, int32_t &date)
{
    //"2022-09-01"
    int year = 0;
    int month = 0;
    int day = 0;
    int ret = sscanf(str, "%d-%d-%d", &year, &month, &day);
    if(ret != 3) {
        return RC::INVALID_ARGUMENT;
    }

    if(year<1900 || year>9999 || (month<=0 || month>12) || (day<=0 || day>31))
    {
        return RC::INVALID_ARGUMENT;
    }

    int max_day_in_month[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const int max_day = max_day_in_month[month-1];
    if(day > max_day) {
        return RC::INVALID_ARGUMENT;
    }

    if(month == 2 && !is_leap_year(year) && day > 28) {
        return RC::INVALID_ARGUMENT;
    }

    date = year*10000 + month*100 + day;  //20220901
    return RC::SUCCESS;
}

inline std::string date_to_string(int32_t date)
{
    //2022-09-01
    std::stringstream ss;
    int year = date / 10000;
    int month = date % 10000 / 100;
    int day = date % 100;
    ss << year ;
    if(month<10) {
        ss << "-" << "0" << month;
    }
    else {
        ss << "-" <<  month;
    }
    if(day<10) {
        ss << "-" << "0" << day;
    }
    else {
        ss << "-" <<  day;
    }
    return ss.str();
}