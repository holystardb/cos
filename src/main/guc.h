#ifndef _GUC_H
#define _GUC_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_attribute.h"

#ifdef __cplusplus
//extern "C" {
#endif // __cplusplus

#define GUC_TYPE_BOOL            1
#define GUC_TYPE_INT32           2
#define GUC_TYPE_INT64           3
#define GUC_TYPE_REAL            4
#define GUC_TYPE_STRING          5
#define GUC_TYPE_ENUM            6

#define GUC_CONTEXT_INTERNAL     1
#define GUC_CONTEXT_POSTMASTER   2
#define GUC_CONTEXT_SIGHUP       3
#define GUC_CONTEXT_USERSET      4

#define GUC_UNIT_NONE       0x0000
#define GUC_UNIT_INVALID    0xFFFF

#define GUC_UNIT_BYTE       0x0001  /* value is in bytes */
#define GUC_UNIT_KB         0x0002  /* value is in kilobytes */
#define GUC_UNIT_MB         0x0003  /* value is in megabytes */
#define GUC_UNIT_GB         0x0004  /* value is in kilobytes */

#define GUC_UNIT_MS         0x0001  /* value is in milliseconds */
#define GUC_UNIT_S          0x0002  /* value is in seconds */
#define GUC_UNIT_MIN        0x0003  /* value is in minutes */
#define GUC_UNIT_HOUR       0x0004  /* value is in hours */
#define GUC_UNIT_DAY        0x0005  /* value is in days */



typedef bool32 (*GucBoolCheckHook) (char *newval, void *config);
typedef bool32 (*GucInt32CheckHook) (char *newval, void *config);
typedef bool32 (*GucInt64CheckHook) (char *newval, void *config);
typedef bool32 (*GucRealCheckHook) (char *newval, void *config);
typedef bool32 (*GucStringCheckHook) (char *newval, void *config);
typedef bool32 (*GucEnumCheckHook) (char *newval, void *config);

typedef void (*GucBoolAssignHook) (char *newval, void *extra);
typedef void (*GucInt32AssignHook) (char *newval, void *extra);
typedef void (*GucInt64AssignHook) (char *newval, void *extra);
typedef void (*GucRealAssignHook) (char *newval, void *extra);
typedef void (*GucStringAssignHook) (char *newval, void **extra);
typedef void (*GucEnumAssignHook) (char *newval, void *extra);

typedef void (*GucBoolInitHook) (bool32 newval, void *config);
typedef void (*GucInt32InitHook) (int32 newval, void *config);
typedef void (*GucInt64InitHook) (int64 newval, void *config);
typedef void (*GucRealInitHook) (double newval, void *config);
typedef void (*GucStringInitHook) (char* newval, void *config);
typedef void (*GucEnumInitHook) (int32 newval, void *config);

typedef const char *(*GucShowHook) (void);

typedef struct {
    char             *name; /* name of variable - MUST BE FIRST */
    char             *desc; /* short desc. of this variable's purpose */
    int32             context;
    int32             type; /* type of variable (set only at startup) */
    int32             flags;
    void             *extra; /* "extra" pointer for current actual value */
} config_generic;

typedef struct
{
    config_generic        gen;
    bool32*               variable;
    bool32                boot_val;
    GucBoolCheckHook      check_hook;
    GucBoolAssignHook     assign_hook;
    GucBoolInitHook       init_hook;
    GucShowHook           show_hook;
} config_bool;

typedef struct
{
    config_generic        gen;
    int32*                variable;
    int32                 boot_val;
    int32                 min;
    int32                 max;
    GucInt32CheckHook     check_hook;
    GucInt32AssignHook    assign_hook;
    GucInt32InitHook      init_hook;
    GucShowHook           show_hook;
} config_int32;

typedef struct
{
    config_generic        gen;
    int64*                variable;
    int64                 boot_val;
    int64                 min;
    int64                 max;
    GucInt64CheckHook     check_hook;
    GucInt64AssignHook    assign_hook;
    GucInt64InitHook      init_hook;
    GucShowHook           show_hook;
} config_int64;

typedef struct
{
    config_generic        gen;
    double*               variable;
    double                boot_val;
    double                min;
    double                max;
    GucRealCheckHook      check_hook;
    GucRealAssignHook     assign_hook;
    GucRealInitHook       init_hook;
    GucShowHook           show_hook;
} config_real;

typedef struct
{
    config_generic        gen;
    char**                variable;
    char*                 boot_val;
    GucStringCheckHook    check_hook;
    GucStringAssignHook   assign_hook;
    GucStringInitHook     init_hook;
    GucShowHook           show_hook;
    char*                 reset_val;
} config_string;

typedef struct
{
    config_generic        gen;
    int32*                variable;
    int32                 boot_val;
    GucEnumCheckHook      check_hook;
    GucEnumAssignHook     assign_hook;
    GucEnumInitHook       init_hook;
    GucShowHook           show_hook;
} config_enum;


extern void build_guc_variables(void);
extern config_generic* find_guc_variable(char* key_name);
extern bool32 set_guc_option_value(config_generic* gconfig, char* value);

//------------------------------------------------------------------------------

extern status_t initialize_guc_options(char* config_file, attribute_t* attr);


#ifdef __cplusplus
//}
#endif // __cplusplus





#endif  /* _GUC_H */
