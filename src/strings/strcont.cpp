/* Copyright (c) 2000, 2022, Oracle and/or its affiliates.
   Use is subject to license terms.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   Without limiting anything contained in the foregoing, this file,
   which is part of C Driver for MySQL (Connector/C), is also subject to the
   Universal FOSS Exception, version 1.0, a copy of which can be found at
   http://oss.oracle.com/licenses/universal-foss-exception.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/*  File   : strcont.c
    Author : Monty
    Updated: 1988.07.27
    Defines: strcont()

    strcont(str, set) if str contanies any character in the string set.
    The result is the position of the first found character in str, or NullS
    if there isn't anything found.

*/

//#include <my_global.h>
#include "m_string.h"

char * strcont(const char *str, const char *set)
{
  char * start = (char *) set;

  while (*str)
  {
    while (*set)
    {
      if (*set++ == *str)
	return ((char*) str);
    }
    set=start; str++;
  }
  return (NullS);
} /* strcont */
