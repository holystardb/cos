#include "cm_stacktrace.h"


#include <windows.h>
#include <excpt.h>
#include <imagehlp.h>
#include <bfd.h>
#include <psapi.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdbool.h>

#define BUFFER_MAX (16*1024)

struct bfd_ctx {
    bfd * handle;
    asymbol ** symbol;
};

struct bfd_set {
    char * name;
    struct bfd_ctx * bc;
    struct bfd_set *next;
};

struct find_info {
    asymbol **symbol;
    bfd_vma counter;
    const char *file;
    const char *func;
    unsigned line;
};

struct output_buffer {
    char * buf;
    size_t sz;
    size_t ptr;
};

static void
output_init(struct output_buffer *ob, char * buf, size_t sz)
{
    ob->buf = buf;
    ob->sz = sz;
    ob->ptr = 0;
    ob->buf[0] = '\0';
}

static void
output_print(struct output_buffer *ob, const char * format, ...)
{
    if (ob->sz == ob->ptr)
        return;
    ob->buf[ob->ptr] = '\0';
    va_list ap;
    va_start(ap, format);
    vsnprintf(ob->buf + ob->ptr, ob->sz - ob->ptr, format, ap);
    va_end(ap);

    ob->ptr = strlen(ob->buf + ob->ptr) + ob->ptr;
}

static void
lookup_section(bfd *abfd, asection *sec, void *opaque_data)
{
    struct find_info *data = opaque_data;

    if (data->func)
        return;

    if (!(bfd_get_section_flags(abfd, sec) & SEC_ALLOC))
        return;

    bfd_vma vma = bfd_get_section_vma(abfd, sec);
    if (data->counter < vma || vma + bfd_get_section_size(sec) <= data->counter)
        return;

    bfd_find_nearest_line(abfd, sec, data->symbol, data->counter - vma, &(data->file), &(data->func), &(data->line));
}

static void
find(struct bfd_ctx * b, DWORD offset, const char **file, const char **func, unsigned *line)
{
    struct find_info data;
    data.func = NULL;
    data.symbol = b->symbol;
    data.counter = offset;
    data.file = NULL;
    data.func = NULL;
    data.line = 0;

    bfd_map_over_sections(b->handle, &lookup_section, &data);
    if (file) {
        *file = data.file;
    }
    if (func) {
        *func = data.func;
    }
    if (line) {
        *line = data.line;
    }
}

static int
init_bfd_ctx(struct bfd_ctx *bc, const char * procname, struct output_buffer *ob)
{
    bc->handle = NULL;
    bc->symbol = NULL;

    bfd *b = bfd_openr(procname, 0);
    if (!b) {
        output_print(ob, "Failed to open bfd from (%s)\n", procname);
        return 1;
    }

    int r1 = bfd_check_format(b, bfd_object);
    int r2 = bfd_check_format_matches(b, bfd_object, NULL);
    int r3 = bfd_get_file_flags(b) & HAS_SYMS;

    if (!(r1 && r2 && r3)) {
        bfd_close(b);
        output_print(ob, "Failed to init bfd from (%s)\n", procname);
        return 1;
    }

    void *symbol_table;

    unsigned dummy = 0;
    if (bfd_read_minisymbols(b, FALSE, &symbol_table, &dummy) == 0) {
        if (bfd_read_minisymbols(b, TRUE, &symbol_table, &dummy) < 0) {
            free(symbol_table);
            bfd_close(b);
            output_print(ob, "Failed to read symbols from (%s)\n", procname);
            return 1;
        }
    }

    bc->handle = b;
    bc->symbol = symbol_table;

    return 0;
}

static void
close_bfd_ctx(struct bfd_ctx *bc)
{
    if (bc) {
        if (bc->symbol) {
            free(bc->symbol);
        }
        if (bc->handle) {
            bfd_close(bc->handle);
        }
    }
}

static struct bfd_ctx *
get_bc(struct output_buffer *ob, struct bfd_set *set, const char *procname)
{
    while (set->name) {
        if (strcmp(set->name, procname) == 0) {
            return set->bc;
        }
        set = set->next;
    }
    struct bfd_ctx bc;
    if (init_bfd_ctx(&bc, procname, ob)) {
        return NULL;
    }
    set->next = calloc(1, sizeof(*set));
    set->bc = malloc(sizeof(struct bfd_ctx));
    memcpy(set->bc, &bc, sizeof(bc));
    set->name = strdup(procname);

    return set->bc;
}

static void
release_set(struct bfd_set *set)
{
    while (set) {
        struct bfd_set * temp = set->next;
        free(set->name);
        close_bfd_ctx(set->bc);
        free(set);
        set = temp;
    }
}

#ifdef WIN64
#define REG_BP Rbp
#define REG_IP Rip
#define REG_SP Rsp
#define IMAGE_FILE_MACHINE_NATIVE IMAGE_FILE_MACHINE_AMD64
#else
#define REG_BP Ebp
#define REG_IP Eip
#define REG_SP Esp
#define IMAGE_FILE_MACHINE_NATIVE IMAGE_FILE_MACHINE_I386
#endif

static void
_backtrace(struct output_buffer *ob, struct bfd_set *set, int depth, LPCONTEXT context)
{
    char procname[MAX_PATH];
    GetModuleFileNameA(NULL, procname, sizeof procname);

    struct bfd_ctx *bc = NULL;

    STACKFRAME frame;
    memset(&frame, 0, sizeof(frame));

    frame.AddrPC.Offset = context->REG_IP;
    frame.AddrPC.Mode = AddrModeFlat;
    frame.AddrStack.Offset = context->REG_SP;
    frame.AddrStack.Mode = AddrModeFlat;
    frame.AddrFrame.Offset = context->REG_BP;
    frame.AddrFrame.Mode = AddrModeFlat;

    HANDLE process = GetCurrentProcess();
    HANDLE thread = GetCurrentThread();

    char symbol_buffer[sizeof(IMAGEHLP_SYMBOL) + 255];
    char module_name_raw[MAX_PATH];

    while (StackWalk(IMAGE_FILE_MACHINE_NATIVE,
        process,
        thread,
        &frame,
        context,
        0,
        SymFunctionTableAccess,
        SymGetModuleBase, 0)) {

        --depth;
        if (depth < 0)
            break;

        IMAGEHLP_SYMBOL *symbol = (IMAGEHLP_SYMBOL *)symbol_buffer;
        symbol->SizeOfStruct = (sizeof *symbol) + 255;
        symbol->MaxNameLength = 254;

        HINSTANCE module_base = (HINSTANCE)SymGetModuleBase(process, frame.AddrPC.Offset);

        const char * module_name = "[unknown module]";
        if (module_base &&
            GetModuleFileNameA(module_base, module_name_raw, MAX_PATH)) {
            module_name = module_name_raw;
            bc = get_bc(ob, set, module_name);
        }

        const char * file = NULL;
        const char * func = NULL;
        unsigned line = 0;

        if (bc) {
            find(bc, frame.AddrPC.Offset, &file, &func, &line);
        }

        if (file == NULL) {
            if (SymGetSymFromAddr(process, frame.AddrPC.Offset, NULL, symbol)) {
                file = symbol->Name;
            } else {
                file = "[unknown file]";
            }
        }
        if (func == NULL) {
            output_print(ob, "0x%x : %s : %s \n",
                frame.AddrPC.Offset,
                module_name,
                file);
        } else {
            output_print(ob, "0x%x : %s : %s (%d) : in function (%s) \n",
                frame.AddrPC.Offset,
                module_name,
                file,
                line,
                func);
        }
    }
}

static char * g_output = NULL;
static LPTOP_LEVEL_EXCEPTION_FILTER g_prev = NULL;

static LONG WINAPI
exception_filter(LPEXCEPTION_POINTERS info)
{
    struct output_buffer ob;
    output_init(&ob, g_output, BUFFER_MAX);

    if (!SymInitialize(GetCurrentProcess(), 0, TRUE)) {
        output_print(&ob, "Failed to init symbol context\n");
    } else {
        bfd_init();
        struct bfd_set *set = calloc(1, sizeof(*set));
        _backtrace(&ob, set, 128, info->ContextRecord);
        release_set(set);

        SymCleanup(GetCurrentProcess());
    }

    fputs(g_output, stderr);

    exit(1);

    return 0;
}

static void
backtrace_register(void)
{
    if (g_output == NULL) {
        g_output = malloc(BUFFER_MAX);
        g_prev = SetUnhandledExceptionFilter(exception_filter);
    }
}

static void
backtrace_unregister(void)
{
    if (g_output) {
        free(g_output);
        SetUnhandledExceptionFilter(g_prev);
        g_prev = NULL;
        g_output = NULL;
    }
}
