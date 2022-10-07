#ifndef _CM_GETOPT_H_
#define _CM_GETOPT_H_

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#ifndef __WIN__

#include <getopt.h>
#include <unistd.h>

#else

int getopt(int argc, char* const argv[], const char* optstring);

extern char *optarg;
extern int   optind,
             opterr,
             optopt;

#define no_argument             0
#define required_argument       1
#define optional_argument       2

struct option
{
    const char *name;
    int         has_arg;
    int        *flag;
    int         val;
};

int getopt_long(int argc,
                    char* const argv[],
                    const char* optstring,
                    const struct option* longopts,
                    int* longindex);

#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _CM_GETOPT_H_