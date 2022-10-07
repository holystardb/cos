#include "cm_error.h"

/* Max length of a error message. */
#define ERRMSGSIZE      (512)

void (*error_handler_hook)(uint32 error, const char *str) = my_message_stderr;


static struct my_err_head
{
  struct my_err_head    *meh_next;         /* chain link */
  const char**          (*get_errmsgs) (); /* returns error message format */
  int                   meh_first;       /* error number matching array slot 0 */
  int                   meh_last;          /* error number matching last slot */
} my_errmsgs_globerrs = {NULL, get_global_errmsgs, EE_ERROR_FIRST, EE_ERROR_LAST};

static struct my_err_head *my_errmsgs_list = &my_errmsgs_globerrs;


const char **get_global_errmsgs()
{
  return glob_error_messages;
}


const char* ut_strerr(dberr_t num) /*!< in: error number */
{
    switch (num) {
    case DB_SUCCESS:
        return("Success");
    case DB_SUCCESS_LOCKED_REC:
        return("Success, record lock created");
    case DB_ERROR:
        return("Generic error");
    case DB_IO_ERROR:
        return("I/O error");

    /* do not add default: in order to produce a warning if new code
    is added to the enum but not added here */
    }

    /* we abort here because if unknown error code is given, this could
    mean that memory corruption has happened and someone's error-code
    variable has been overwritten with bogus data */
    ut_error;

    /* NOT REACHED */
    return("Unknown error");
}


char *strmake(register char *dst, register const char *src, size_t length)
{
    while (length--) {
        if (! (*dst++ = *src++)) {
            return dst-1;
        }
    }
    *dst=0;
    return dst;
}


/**
  Get a string describing a system or handler error. thread-safe.

  @param  buf  a buffer in which to return the error message
  @param  len  the size of the aforementioned buffer
  @param  nr   the error number

  @retval buf  always buf. for signature compatibility with strerror(3).
*/

char *my_strerror(char *buf, size_t len, int nr)
{
    char *msg = NULL;

    buf[0]= '\0';                                  /* failsafe */

    //  These (handler-) error messages are shared by perror, as required by the principle of least surprise.

    if ((nr >= EE_ERROR_FIRST) && (nr <= EE_ERROR_LAST)) {
        msg= (char *) glob_error_messages[nr - EE_ERROR_FIRST];
    }

    if (msg != NULL) {
        strmake(buf, msg, len - 1);
    }
    else
    {
#if defined(__WIN__)
        strerror_s(buf, len, nr);
#else
        strerror_r(nr, buf, len);
#endif
    }

    // strerror() return values are implementation-dependent, so let's be pragmatic.
    if (!buf[0]) {
        strmake(buf, "unknown error", len - 1);
    }

    return buf;
}




/**
  @brief Get an error format string from one of the my_error_register()ed sets

  @note
    NULL values are possible even within a registered range.

  @param nr Errno

  @retval NULL  if no message is registered for this error number
  @retval str   C-string
*/

const char *my_get_err_msg(int nr)
{
  const char *format;
  struct my_err_head *meh_p;

  /* Search for the range this error is in. */
  for (meh_p = my_errmsgs_list; meh_p; meh_p= meh_p->meh_next)
    if (nr <= meh_p->meh_last)
      break;

  /*
    If we found the range this error number is in, get the format string.
    If the string is empty, or a NULL pointer, or if we're out of return, we return NULL.
  */
  if (!(format= (meh_p && (nr >= meh_p->meh_first)) ? meh_p->get_errmsgs()[nr - meh_p->meh_first] : NULL) || !*format)
    return NULL;

  return format;
}

/**
  Register error messages for use with my_error().

  @description

    The pointer array is expected to contain addresses to NUL-terminated
    C character strings. The array contains (last - first + 1) pointers.
    NULL pointers and empty strings ("") are allowed. These will be mapped to
    "Unknown error" when my_error() is called with a matching error number.
    This function registers the error numbers 'first' to 'last'.
    No overlapping with previously registered error numbers is allowed.

  @param   errmsgs  array of pointers to error messages
  @param   first    error number of first message in the array
  @param   last     error number of last message in the array

  @retval  0        OK
  @retval  != 0     Error
*/

int my_error_register(const char** (*get_errmsgs) (), int first, int last)
{
  struct my_err_head *meh_p;
  struct my_err_head **search_meh_pp;

  /* Allocate a new header structure. */
  if (! (meh_p= (struct my_err_head*) my_malloc(sizeof(struct my_err_head),
                                                MYF(MY_WME))))
    return 1;
  meh_p->get_errmsgs= get_errmsgs;
  meh_p->meh_first= first;
  meh_p->meh_last= last;

  /* Search for the right position in the list. */
  for (search_meh_pp= &my_errmsgs_list;
       *search_meh_pp;
       search_meh_pp= &(*search_meh_pp)->meh_next)
  {
    if ((*search_meh_pp)->meh_last > first)
      break;
  }

  /* Error numbers must be unique. No overlapping is allowed. */
  if (*search_meh_pp && ((*search_meh_pp)->meh_first <= last))
  {
    my_free(meh_p);
    return 1;
  }

  /* Insert header into the chain. */
  meh_p->meh_next= *search_meh_pp;
  *search_meh_pp= meh_p;
  return 0;
}


/**
  Unregister formerly registered error messages.

  @description

    This function unregisters the error numbers 'first' to 'last'.
    These must have been previously registered by my_error_register().
    'first' and 'last' must exactly match the registration.
    If a matching registration is present, the header is removed from the
    list and the pointer to the error messages pointers array is returned.
    (The messages themselves are not released here as they may be static.)
    Otherwise, NULL is returned.

  @param   first     error number of first message
  @param   last      error number of last message

  @retval  NULL      Error, no such number range registered.
  @retval  non-NULL  OK, returns address of error messages pointers array.
*/

const char **my_error_unregister(int first, int last)
{
  struct my_err_head    *meh_p;
  struct my_err_head    **search_meh_pp;
  const char            **errmsgs;

  /* Search for the registration in the list. */
  for (search_meh_pp= &my_errmsgs_list;
       *search_meh_pp;
       search_meh_pp= &(*search_meh_pp)->meh_next)
  {
    if (((*search_meh_pp)->meh_first == first) &&
        ((*search_meh_pp)->meh_last == last))
      break;
  }
  if (! *search_meh_pp)
    return NULL;

  /* Remove header from the chain. */
  meh_p= *search_meh_pp;
  *search_meh_pp= meh_p->meh_next;

  /* Save the return value and free the header. */
  errmsgs= meh_p->get_errmsgs();
  my_free(meh_p);
  
  return errmsgs;
}

/**
  Read messages from errorfile.

  This function can be called multiple times to reload the messages.
  If it fails to load the messages, it will fail softly by initializing
  the errmesg pointer to an array of empty strings or by keeping the
  old array if it exists.

  @retval
    FALSE       OK
  @retval
    TRUE        Error
*/
#define ERRMSG_FILE         "errmsg.sys"

bool init_errmessage(void)
{
  const char **errmsgs, **ptr;
  DBUG_ENTER("init_errmessage");

  /*
    Get a pointer to the old error messages pointer array.
    read_texts() tries to free it.
  */
  errmsgs= my_error_unregister(EE_ERROR_FIRST, EE_ERROR_LAST);

  /* Read messages from file. */
  if (read_texts(ERRMSG_FILE, my_default_lc_messages->errmsgs->language,
                 &errmsgs, EE_ERROR_LAST - EE_ERROR_FIRST + 1) &&
      !errmsgs)
  {
    if (!(errmsgs= (const char**) my_malloc((EE_ERROR_LAST - EE_ERROR_FIRST+1) * sizeof(char*))))
      DBUG_RETURN(TRUE);
    for (ptr= errmsgs; ptr < errmsgs + ER_ERROR_LAST - ER_ERROR_FIRST; ptr++)
	  *ptr= "";
  }

  /* Register messages for use with my_error(). */
  if (my_error_register(get_server_errmsgs, ER_ERROR_FIRST, ER_ERROR_LAST))
  {
    my_free(errmsgs);
    DBUG_RETURN(TRUE);
  }

  DEFAULT_ERRMSGS= errmsgs;             /* Init global variable */
  init_myfunc_errs();			/* Init myfunc messages */
  DBUG_RETURN(FALSE);
}


/**
  Read text from packed textfile in language-directory.

  If we can't read messagefile then it's panic- we can't continue.
*/

bool read_texts(const char *file_name, const char *language,
                const char ***point, uint error_messages)
{
  register uint i;
  uint count,funktpos,textcount;
  size_t length;
  File file;
  char name[FN_REFLEN];
  char lang_path[FN_REFLEN];
  uchar *buff;
  uchar head[32],*pos;
  DBUG_ENTER("read_texts");

  LINT_INIT(buff);
  funktpos=0;
  convert_dirname(lang_path, language, NullS);
  (void) my_load_path(lang_path, lang_path, lc_messages_dir);
  if ((file= mysql_file_open(key_file_ERRMSG,
                             fn_format(name, file_name, lang_path, "", 4),
                             O_RDONLY | O_SHARE | O_BINARY,
                             MYF(0))) < 0)
  {
    /*
      Trying pre-5.4 sematics of the --language parameter.
      It included the language-specific part, e.g.:
      
      --language=/path/to/english/
    */
    if ((file= mysql_file_open(key_file_ERRMSG,
                               fn_format(name, file_name, lc_messages_dir, "", 4),
                               O_RDONLY | O_SHARE | O_BINARY,
                               MYF(0))) < 0)
      goto err;

    sql_print_warning("Using pre 5.5 semantics to load error messages from %s.",
                      lc_messages_dir);
    
    sql_print_warning("If this is not intended, refer to the documentation for "
                      "valid usage of --lc-messages-dir and --language "
                      "parameters.");
  }

  funktpos=1;
  if (mysql_file_read(file, (uchar*) head, 32, MYF(MY_NABP)))
    goto err;
  if (head[0] != (uchar) 254 || head[1] != (uchar) 254 ||
      head[2] != 3 || head[3] != 1)
    goto err; /* purecov: inspected */
  textcount=head[4];

  error_message_charset_info= system_charset_info;
  length=uint4korr(head+6); count=uint4korr(head+10);

  if (count < error_messages)
  {
    sql_print_error("\
Error message file '%s' had only %d error messages,\n\
but it should contain at least %d error messages.\n\
Check that the above file is the right version for this program!",
		    name,count,error_messages);
    (void) mysql_file_close(file, MYF(MY_WME));
    DBUG_RETURN(1);
  }

  /* Free old language */
  my_free(*point);
  if (!(*point= (const char**)
	my_malloc((size_t) (length+count*sizeof(char*)),MYF(0))))
  {
    funktpos=2;					/* purecov: inspected */
    goto err;					/* purecov: inspected */
  }
  buff= (uchar*) (*point + count);

  if (mysql_file_read(file, buff, (size_t) count*4, MYF(MY_NABP)))
    goto err;
  for (i=0, pos= buff ; i< count ; i++)
  {
    (*point)[i]= (char*) buff+uint4korr(pos);
    pos+=4;
  }
  if (mysql_file_read(file, buff, length, MYF(MY_NABP)))
    goto err;

  for (i=1 ; i < textcount ; i++)
  {
    point[i]= *point +uint2korr(head+10+i+i);
  }
  (void) mysql_file_close(file, MYF(0));
  DBUG_RETURN(0);

err:
  sql_print_error((funktpos == 2) ? "Not enough memory for messagefile '%s'" :
                  ((funktpos == 1) ? "Can't read from messagefile '%s'" :
                   "Can't find messagefile '%s'"), name);
  if (file != FERR)
    (void) mysql_file_close(file, MYF(MY_WME));
  DBUG_RETURN(1);
} /* read_texts */


/* Macros for converting *constants* to the right type */
#define MYF(v)      (int)(v)

#define MY_FFNF		1	/* Fatal if file not found */
#define MY_FNABP	2	/* Fatal if not all bytes read/writen */
#define MY_NABP		4	/* Error if not all bytes read/writen */
#define MY_FAE		8	/* Fatal if any error */
#define MY_WME		16	/* Write message on error */


/**
  Fill in and print a previously registered error message.

  @note
    Goes through the (sole) function registered in error_handler_hook

  @param nr        error number
  @param MyFlags   Flags
  @param ...       variable list matching that error format string
*/
void my_error(int nr, myf MyFlags, ...)
{
    const char *format;
    va_list args;
    char ebuff[ERRMSGSIZE];

    DBUG_ENTER("my_error");
    DBUG_PRINT("my", ("nr: %d  MyFlags: %d  errno: %d", nr, MyFlags, errno));

    if (!(format = my_get_err_msg(nr))) {
        (void)snprintf(ebuff, sizeof(ebuff), "Unknown error %d", nr);
    }
    else
    {
        va_start(args,MyFlags);
        (void)vsnprintf(ebuff, sizeof(ebuff), format, args);
        va_end(args);
    }
    (*error_handler_hook)(nr, ebuff, MyFlags);
    DBUG_VOID_RETURN;
}


/**
  Print an error message.

  @note
    Goes through the (sole) function registered in error_handler_hook

  @param error     error number
  @param format    format string
  @param MyFlags   Flags
  @param ...       variable list matching that error format string
*/
void my_printf_error(uint error, const char *format, myf MyFlags, ...)
{
    va_list args;
    char ebuff[ERRMSGSIZE];
    DBUG_ENTER("my_printf_error");
    DBUG_PRINT("my", ("nr: %d  MyFlags: %d  errno: %d  Format: %s", error, MyFlags, errno, format));

    va_start(args,MyFlags);
    (void) my_vsnprintf_ex(&my_charset_utf8_general_ci, ebuff, sizeof(ebuff), format, args);
    va_end(args);
    (*error_handler_hook)(error, ebuff, MyFlags);
    DBUG_VOID_RETURN;
}

void my_message_stderr(uint error __attribute__((unused)), const char *str)
{
    (void)fflush(stdout);
    (void)fputs(str,stderr);
    (void)fputc('\n',stderr);
    (void)fflush(stderr);
}

