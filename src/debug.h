#ifndef _DEBUG_H
#define _DEBUG_H
/*
 *    FileName:debug.h
 *    
 */

#define		LV_ERR				0		//<推荐使用>  运行过程中出现的错误,对系统正常运行有影响
#define		LV_WARN				1		// 警告信息
#define		LV_INFO				2		//<推荐使用>  提示信息
#define		LV_DEBUG			3		//<推荐使用>  调试信息
#define		LV_NONE			    4		//<推荐使用>  不输出

#define 	Debug_Level   3		 //限制调试信息出处的宏

static const char *Debuginfo[] = {
    "Error", "Warning", "Info", "Debbug", "None"
};

#define PRINT printf


//	程序中请使用 print_log 宏
#define print_log(level, format, a...)  \
do{                                 \
	if (level <= Debug_Level)  {    \
        PRINT("%-7s:%-20s %3d:"#format"\n", Debuginfo[level], __FUNCTION__, __LINE__, ##a);\
	}\
}while(0)	

#endif

