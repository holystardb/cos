#include "cm_locale.h"

MY_LOCALE *my_default_lc_messages;
MY_LOCALE *my_default_lc_time_names;

void init()
{
    static char *lc_messages;
    static char *lc_time_names_name;

    lc_messages= (char*) "en_US";
    lc_time_names_name= (char*) "en_US";

    if (!(my_default_lc_time_names =  my_locale_by_name(lc_time_names_name)))
    {
        LOG_PRINT_ERROR("Unknown locale: '%s'", lc_time_names_name);
        return;
    }

    if (!(my_default_lc_messages =  my_locale_by_name(lc_messages)))
    {
        LOG_PRINT_ERROR("Unknown locale: '%s'", lc_messages);
        return;
    }

}

#define array_elements(A) ((uint) (sizeof(A)/sizeof(A[0])))
#define NullS             (char *) 0

enum err_msgs_index
{
  en_US= 0, cs_CZ, da_DK, nl_NL, et_EE, fr_FR, de_DE, el_GR, hu_HU, it_IT,
  ja_JP, ko_KR, no_NO, nn_NO, pl_PL, pt_PT, ro_RO, ru_RU, sr_RS,  sk_SK,
  es_ES, sv_SE, uk_UA
} ERR_MSGS_INDEX;

MY_LOCALE_ERRMSGS global_errmsgs[]=
{
  {"english", NULL},
  {"czech", NULL},
  {"danish", NULL},
  {"dutch", NULL},
  {"estonian", NULL},
  {"french", NULL},
  {"german", NULL},
  {"greek", NULL},
  {"hungarian", NULL},
  {"italian", NULL},
  {"japanese", NULL},
  {"korean", NULL},
  {"norwegian", NULL},
  {"norwegian-ny", NULL},
  {"polish", NULL},
  {"portuguese", NULL},
  {"romanian", NULL},
  {"russian", NULL},
  {"serbian", NULL},
  {"slovak", NULL},
  {"spanish", NULL},
  {"swedish", NULL},
  {"ukrainian", NULL},
  {NULL, NULL}
};


/***** LOCALE BEGIN en_US: English - United States *****/
static const char *my_locale_month_names_en_US[13] = 
 {"January","February","March","April","May","June","July","August","September","October","November","December", NullS };
static const char *my_locale_ab_month_names_en_US[13] = 
 {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec", NullS };
static const char *my_locale_day_names_en_US[8] = 
 {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday", NullS };
static const char *my_locale_ab_day_names_en_US[8] = 
 {"Mon","Tue","Wed","Thu","Fri","Sat","Sun", NullS };
static TYPELIB my_locale_typelib_month_names_en_US = 
 { array_elements(my_locale_month_names_en_US)-1, "", my_locale_month_names_en_US, NULL };
static TYPELIB my_locale_typelib_ab_month_names_en_US = 
 { array_elements(my_locale_ab_month_names_en_US)-1, "", my_locale_ab_month_names_en_US, NULL };
static TYPELIB my_locale_typelib_day_names_en_US = 
 { array_elements(my_locale_day_names_en_US)-1, "", my_locale_day_names_en_US, NULL };
static TYPELIB my_locale_typelib_ab_day_names_en_US = 
 { array_elements(my_locale_ab_day_names_en_US)-1, "", my_locale_ab_day_names_en_US, NULL };
MY_LOCALE my_locale_en_US
(
  0,
  "en_US",
  "English - United States",
  TRUE,
  &my_locale_typelib_month_names_en_US,
  &my_locale_typelib_ab_month_names_en_US,
  &my_locale_typelib_day_names_en_US,
  &my_locale_typelib_ab_day_names_en_US,
  9,
  9,
  '.',        /* decimal point en_US */
  ',',        /* thousands_sep en_US */
  "\x03\x03", /* grouping      en_US */
  &global_errmsgs[en_US]
);
/***** LOCALE END en_US *****/


/***** LOCALE BEGIN zh_CN: Chinese - Peoples Republic of China *****/
static const char *my_locale_month_names_zh_CN[13] = 
 {"涓€�?,"浜屾湀","涓夋湀","鍥涙湀","浜旀湀","鍏湀","涓冩湀","鍏湀","涔濇湀","鍗佹湀","鍗佷竴鏈?,"鍗佷簩鏈?, NullS };
static const char *my_locale_ab_month_names_zh_CN[13] = 
 {" 1�?," 2�?," 3�?," 4�?," 5�?," 6�?," 7�?," 8�?," 9�?,"10�?,"11�?,"12�?, NullS };
static const char *my_locale_day_names_zh_CN[8] = 
 {"鏄熸湡涓�?,"鏄熸湡浜?,"鏄熸湡涓?,"鏄熸湡鍥?,"鏄熸湡浜?,"鏄熸湡鍏?,"鏄熸湡鏃?, NullS };
static const char *my_locale_ab_day_names_zh_CN[8] = 
 {"涓€","�?,"�?,"�?,"�?,"�?,"�?, NullS };
static TYPELIB my_locale_typelib_month_names_zh_CN = 
 { array_elements(my_locale_month_names_zh_CN)-1, "", my_locale_month_names_zh_CN, NULL };
static TYPELIB my_locale_typelib_ab_month_names_zh_CN = 
 { array_elements(my_locale_ab_month_names_zh_CN)-1, "", my_locale_ab_month_names_zh_CN, NULL };
static TYPELIB my_locale_typelib_day_names_zh_CN = 
 { array_elements(my_locale_day_names_zh_CN)-1, "", my_locale_day_names_zh_CN, NULL };
static TYPELIB my_locale_typelib_ab_day_names_zh_CN = 
 { array_elements(my_locale_ab_day_names_zh_CN)-1, "", my_locale_ab_day_names_zh_CN, NULL };
MY_LOCALE my_locale_zh_CN
(
  56,
  "zh_CN",
  "Chinese - Peoples Republic of China",
  FALSE,
  &my_locale_typelib_month_names_zh_CN,
  &my_locale_typelib_ab_month_names_zh_CN,
  &my_locale_typelib_day_names_zh_CN,
  &my_locale_typelib_ab_day_names_zh_CN,
  3,
  3,
  '.',        /* decimal point zh_CN */
  ',',        /* thousands_sep zh_CN */
  "\x03",     /* grouping      zh_CN */
  &global_errmsgs[en_US]
);
/***** LOCALE END zh_CN *****/

MY_LOCALE *my_locale_by_number(uint number)
{
  MY_LOCALE *locale;
  if (number >= array_elements(my_locales) - 1)
    return NULL;
  locale= my_locales[number];
  // Check that locale is on its correct position in the array
  //DBUG_ASSERT(locale == my_locales[locale->number]);
  return locale;
}


static MY_LOCALE*
my_locale_by_name(MY_LOCALE** locales, const char *name)
{
  MY_LOCALE **locale;
  for (locale= locales; *locale != NULL; locale++) 
  {
    if (!my_strcasecmp(&my_charset_latin1, (*locale)->name, name))
      return *locale;
  }
  return NULL;
}

MY_LOCALE *my_locale_by_name(const char *name)
{
  MY_LOCALE *locale;
  
  if ((locale= my_locale_by_name(my_locales, name)))
  {
      // Check that locale is on its correct position in the array
      DBUG_ASSERT(locale == my_locales[locale->number]);
      return locale;
  }
  else if ((locale= my_locale_by_name(my_locales_deprecated, name)))
  {
    THD *thd= current_thd;
    /*
      Replace the deprecated locale to the corresponding
      'fresh' locale with the same ID.
    */
    locale= my_locales[locale->number];
    if (thd)
    {
      // Send a warning to the client
      push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN,
                          ER_WARN_DEPRECATED_SYNTAX, ER(ER_WARN_DEPRECATED_SYNTAX),
                          name, locale->name);
    }
    else
    {
      // Send a warning to mysqld error log
      sql_print_warning("The syntax '%s' is deprecated and will be removed. "
                        "Please use %s instead.",
                        name, locale->name);
    }
  }
  return locale;
}


void cleanup_errmsgs()
{
  for (MY_LOCALE_ERRMSGS *msgs= global_errmsgs; msgs->language; msgs++)
  {
    my_free(msgs->errmsgs);
  }
}

