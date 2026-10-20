
#include <cassert>
#include <cmath>
#include <cfloat>
#include <limits>
#include <initializer_list>
using datetime = long long;
#define EMPTY_VALUE DBL_MAX
enum ENUM_TIMEFRAMES { PERIOD_CURRENT=0, PERIOD_M1=1, PERIOD_M2=2, PERIOD_M3=3, PERIOD_M4=4,
 PERIOD_M5=5, PERIOD_M6=6, PERIOD_M10=10, PERIOD_M12=12, PERIOD_M15=15, PERIOD_M20=20, PERIOD_M30=30,
 PERIOD_H1=16385, PERIOD_H2=16386, PERIOD_H3=16387, PERIOD_H4=16388, PERIOD_H6=16390, PERIOD_H8=16392,
 PERIOD_H12=16396, PERIOD_D1=16408, PERIOD_W1=32769, PERIOD_MN1=49153 };
struct MqlRates { datetime time; double open, high, low, close; long tick_volume; int spread; long real_volume; };
template <typename T> void ZeroMemory(T &value) { value = {}; }
bool MathIsValidNumber(double value) { return std::isfinite(value); }

#include <iostream>
double MathMin(double a,double b){return a<b?a:b;}
double MathFloor(double a){return std::floor(a);}
double MathAbs(double a){return std::abs(a);}
double NormalizeDouble(double a,int digits){double p=std::pow(10.0,digits);return std::round(a*p)/p;}
#include <Framework/HADOUKEN/Contracts/HDK_PartialPolicy.mqh>
int main(){double out;for(int base:{0,1}){assert(HDKPartialVolume(3,2,.5,0,base,.1,100,.1,out));std::cout<<out<<"\n";} }
