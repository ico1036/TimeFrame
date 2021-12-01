#pragma once
#include <algorithm>
#include <string>
#include <stdexcept>
#include <climits>
#include "TChain.h"

typedef long long TimeNS;

constexpr TimeNS T_Milis = 1000000;
constexpr TimeNS T_Second = T_Milis * 1000;
constexpr TimeNS T_Minute = T_Second * 60;
constexpr TimeNS T_Hour = T_Minute * 60;
constexpr TimeNS T_Day = T_Hour * 24;
constexpr TimeNS T_Week = T_Day * 7;
constexpr TimeNS T_Year = T_Day * 365;
constexpr TimeNS T_YearLeap = T_Day * 366;
constexpr TimeNS T_Infinity = LLONG_MAX;

constexpr int StartYear = 2001;

const std::string T_WeekDays[] = { "MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN" };

bool isLeapYear(int year)
{
   if (year % 4 > 0)
   {
      return false;
   }
   else if (year % 100 > 0)
   {
      return true;
   }
   else if (year % 400 > 0)
   {
      return false;
   }
   else
   {
      return true;
   }
}
TimeNS nsOfYear(int year)
{
   if (isLeapYear(year))
   {
      return T_YearLeap;
   }
   else
   {
      return T_Year;
   }
}
TimeNS nsOfMonth(int year, int month)
{
   if (month == 0 || month == 2 || month == 4 || month == 6 || month == 7 || month == 9 || month == 11)
   {
      return 31 * T_Day;
   }
   else if (month == 3 || month == 5 || month == 8 || month == 10)
   {
      return 30 * T_Day;
   }
   else
   {
      if (isLeapYear(year))
      {
         return 29 * T_Day;
      }
      else
      {
         return 28 * T_Day;
      }
   }
}
TimeNS timestampToNS(std::string time, bool removeSymbols = false)
{
   if(removeSymbols)
   {
      time.erase(std::remove_if(time.begin(), time.end(), [](unsigned char x){
         return x == '-' || x == ':' || x == '.';
      }), time.end());   
   }

   if (time.length() < 17)
   {
      throw std::runtime_error("Wrong timestamp: " + time);
   }

   int year = std::stoi(time.substr(0, 4));
   int month = std::stoi(time.substr(4, 2)) - 1;
   int day = std::stoi(time.substr(6, 2)) - 1;
   int hour = std::stoi(time.substr(8, 2));
   int minute = std::stoi(time.substr(10, 2));
   int second = std::stoi(time.substr(12, 2));
   int milis = std::stoi(time.substr(14, 3));
   int micros = time.length() >= 20 ? std::stoi(time.substr(17, 3)) : 0;
   int nanos = time.length() >= 23 ? std::stoi(time.substr(20, 3)) : 0;

   TimeNS ns = 0;

   for (int y = StartYear; y < year; y++)
   {
      ns += nsOfYear(y);
   }

   for (int m = 0; m < month; m++)
   {
      ns += nsOfMonth(year, m);
   }

   ns += day * T_Day;
   ns += hour * T_Hour;
   ns += minute * T_Minute;
   ns += second * T_Second;
   ns += milis * 1000000;
   ns += micros * 1000;
   ns += nanos;

   return ns;
}

TimeNS dateToNS(std::string date)
{
   if (date.length() < 8)
   {
      throw std::runtime_error("Wrong timestamp: " + date);
   }

   int year = std::stoi(date.substr(0, 4));
   int month = std::stoi(date.substr(4, 2)) - 1;
   int day = std::stoi(date.substr(6, 2)) - 1;

   TimeNS ns = 0;

   for (int y = StartYear; y < year; y++)
   {
      ns += nsOfYear(y);
   }

   for (int m = 0; m < month; m++)
   {
      ns += nsOfMonth(year, m);
   }

   ns += day * T_Day;

   return ns;
}

std::string intToStrFixed(int v, int l)
{
   std::string s = std::to_string(v);
   while (s.length() < l)
   {
      s.insert(s.begin(), '0');
   }
   return s;
}

std::string nsToTimestamp(TimeNS ns)
{
   int year = StartYear;
   while (ns >= nsOfYear(year))
   {
      ns -= nsOfYear(year);
      year++;
   }

   int month = 0;
   while (ns >= nsOfMonth(year, month))
   {
      ns -= nsOfMonth(year, month);
      month++;
   }

   int day = ns / T_Day;
   ns -= day * T_Day;

   int hour = ns / T_Hour;
   ns -= hour * T_Hour;

   int minute = ns / T_Minute;
   ns -= minute * T_Minute;

   int second = ns / T_Second;
   ns -= second * T_Second;

   return intToStrFixed(year, 4) +
      intToStrFixed(month + 1, 2) +
      intToStrFixed(day + 1, 2) +
      intToStrFixed(hour, 2) +
      intToStrFixed(minute, 2) +
      intToStrFixed(second, 2) +
      intToStrFixed(ns, 9);
}

std::string nsToDate(TimeNS ns)
{
	int year = StartYear;
	while (ns >= nsOfYear(year))
	{
		ns -= nsOfYear(year);
		year++;
	}

	int month = 0;
	while (ns >= nsOfMonth(year, month))
	{
		ns -= nsOfMonth(year, month);
		month++;
	}

	int day = ns / T_Day;

	return intToStrFixed(year, 4) +
		intToStrFixed(month + 1, 2) +
		intToStrFixed(day + 1, 2);
}

// Input examples: "+0100" "-0600"
TimeNS offsetToNS(std::string offset)
{
   if (offset.length() != 5)
   {
      throw std::runtime_error("Wrong timestamp");
   }

   int hour = std::stoi(offset.substr(1, 2));
   int minute = std::stoi(offset.substr(3, 2));

   TimeNS ms = 0;

   ms += hour * T_Hour;
   ms += minute * T_Minute;

   if (offset[0] == '-')
   {
      ms *= -1;
   }

   return ms;
}

// Input example: "201512"
TimeNS yearMonthToNS(std::string date)
{
   int year = std::stoi(date.substr(0, 4));
   int month = std::stoi(date.substr(4, 2)) - 1;

   TimeNS ns = 0;

   for (int y = StartYear; y < year; y++)
   {
      ns += nsOfYear(y);
   }

   for (int m = 0; m < month; m++)
   {
      ns += nsOfMonth(year, m);
   }

   return ns;
}

// Input examples: "MON090000"
TimeNS weekTimeToNS(std::string time)
{
   if (time.length() != 9)
   {
      throw std::runtime_error("Wrong timestamp: Week Time");
   }

   auto day = time.substr(0, 3);
   int hour = std::stoi(time.substr(3, 2));
   int minute = std::stoi(time.substr(5, 2));
   int second = std::stoi(time.substr(7, 2));

   TimeNS ms = 0;

   for (int i = 0; i < 7; i++)
   {
      if (day == T_WeekDays[i])
      {
         ms += i * T_Day;
      }
   }

   ms += hour * T_Hour;
   ms += minute * T_Minute;
   ms += second * T_Second;

   return ms;
}

const int amountDayLightSavings = 20;
const TimeNS daylightSavingsUS[amountDayLightSavings][2] = {
   {timestampToNS("20000402020000000") - offsetToNS("-0600"), timestampToNS("20001029020000000") - offsetToNS("-0600")},
   {timestampToNS("20010401020000000") - offsetToNS("-0600"), timestampToNS("20011028020000000") - offsetToNS("-0600")},
   {timestampToNS("20020407020000000") - offsetToNS("-0600"), timestampToNS("20021027020000000") - offsetToNS("-0600")},
   {timestampToNS("20030406020000000") - offsetToNS("-0600"), timestampToNS("20031026020000000") - offsetToNS("-0600")},
   {timestampToNS("20040404020000000") - offsetToNS("-0600"), timestampToNS("20041031020000000") - offsetToNS("-0600")},
   {timestampToNS("20050403020000000") - offsetToNS("-0600"), timestampToNS("20051030020000000") - offsetToNS("-0600")},
   {timestampToNS("20060402020000000") - offsetToNS("-0600"), timestampToNS("20061029020000000") - offsetToNS("-0600")},
   {timestampToNS("20070311020000000") - offsetToNS("-0600"), timestampToNS("20071104020000000") - offsetToNS("-0600")},
   {timestampToNS("20080309020000000") - offsetToNS("-0600"), timestampToNS("20081102020000000") - offsetToNS("-0600")},
   {timestampToNS("20090308020000000") - offsetToNS("-0600"), timestampToNS("20091101020000000") - offsetToNS("-0600")},

   {timestampToNS("20100314020000000") - offsetToNS("-0600"), timestampToNS("20101107020000000") - offsetToNS("-0600")},
   {timestampToNS("20110313020000000") - offsetToNS("-0600"), timestampToNS("20111106020000000") - offsetToNS("-0600")},
   {timestampToNS("20120311020000000") - offsetToNS("-0600"), timestampToNS("20121104020000000") - offsetToNS("-0600")},
   {timestampToNS("20130310020000000") - offsetToNS("-0600"), timestampToNS("20131103020000000") - offsetToNS("-0600")},
   {timestampToNS("20140309020000000") - offsetToNS("-0600"), timestampToNS("20141102020000000") - offsetToNS("-0600")},
   {timestampToNS("20150308020000000") - offsetToNS("-0600"), timestampToNS("20151101020000000") - offsetToNS("-0600")},
   {timestampToNS("20160313020000000") - offsetToNS("-0600"), timestampToNS("20161106020000000") - offsetToNS("-0600")},
   {timestampToNS("20170312020000000") - offsetToNS("-0600"), timestampToNS("20171105020000000") - offsetToNS("-0600")},
   {timestampToNS("20180311020000000") - offsetToNS("-0600"), timestampToNS("20181104020000000") - offsetToNS("-0600")},
   {timestampToNS("20190310020000000") - offsetToNS("-0600"), timestampToNS("20191103020000000") - offsetToNS("-0600")},
};

bool isDaylightSaving(TimeNS time, std::string& continent)
{
   if(continent == "US")
   {
      for (int i = 0; i < amountDayLightSavings; i++)
      {
         if (daylightSavingsUS[i][0] <= time && time <= daylightSavingsUS[i][1])
         {
            return true;
         }
      }
      return false;
   }
   throw std::runtime_error("Continent not found");
}
