#ifndef _OS_MACH_H
#define _OS_MACH_H

#include "os_type.h"

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************//**
The following function is used to store data in one char. */
void mach_write_to_1(
	unsigned char*	b,	/*!< in: pointer to char where to store */
	uint32	n);	 /*!< in: uint32 integer to be stored, >= 0, < 256 */
	
/********************************************************//**
The following function is used to fetch data from one char.
@return	uint32 integer, >= 0, < 256 */
uint32 mach_read_from_1(
	const unsigned char*	b);	/*!< in: pointer to char */
	
/*******************************************************//**
The following function is used to store data in two consecutive
bytes. We store the most significant char to the lower address. */
void mach_write_to_2(
	unsigned char*	b,	/*!< in: pointer to two bytes where to store */
	uint32	n);	 /*!< in: uint32 integer to be stored, >= 0, < 64k */
	
/********************************************************//**
The following function is used to fetch data from two consecutive
bytes. The most significant char is at the lowest address.
@return	uint32 integer, >= 0, < 64k */
uint32 mach_read_from_2(
/*=============*/
	const unsigned char*	b);	/*!< in: pointer to two bytes */
	
/*******************************************************//**
The following function is used to store data in 3 consecutive
bytes. We store the most significant char to the lowest address. */
void mach_write_to_3(

	unsigned char*	b,	/*!< in: pointer to 3 bytes where to store */
	uint32	n);	 /*!< in: uint32 integer to be stored */
	
/********************************************************//**
The following function is used to fetch data from 3 consecutive
bytes. The most significant char is at the lowest address.
@return	uint32 integer */
uint32 mach_read_from_3(
/*=============*/
	const unsigned char*	b);	/*!< in: pointer to 3 bytes */
	
/*******************************************************//**
The following function is used to store data in four consecutive
bytes. We store the most significant char to the lowest address. */
void mach_write_to_4(
	unsigned char*	b,	/*!< in: pointer to four bytes where to store */
	uint32	n);	 /*!< in: uint32 integer to be stored */
	
/********************************************************//**
The following function is used to fetch data from 4 consecutive
bytes. The most significant char is at the lowest address.
@return	uint32 integer */
uint32 mach_read_from_4(
	const unsigned char*	b);	/*!< in: pointer to four bytes */
	
/*********************************************************//**
Writes a uint32 in a compressed form (1..5 bytes).
@return	stored size in bytes */
uint32 mach_write_compressed(
	unsigned char*	b,	/*!< in: pointer to memory where to store */
	uint32	n);	/*!< in: uint32 integer to be stored */
	
/*********************************************************//**
Returns the size of an uint32 when written in the compressed form.
@return	compressed size in bytes */
uint32 mach_get_compressed_size(
	uint32	n);	/*!< in: uint32 integer to be stored */
	
/*********************************************************//**
Reads a uint32 in a compressed form.
@return	read integer */
uint32 mach_read_compressed(
	const unsigned char*	b);	/*!< in: pointer to memory from where to read */
	
/*******************************************************//**
The following function is used to store data in 6 consecutive
bytes. We store the most significant char to the lowest address. */
void mach_write_to_6(
	unsigned char*	b,	/*!< in: pointer to 6 bytes where to store */
	uint64	id);	/*!< in: 48-bit integer */
	
/********************************************************//**
The following function is used to fetch data from 6 consecutive
bytes. The most significant char is at the lowest address.
@return	48-bit integer */
uint64 mach_read_from_6(
	const unsigned char*	b);	/*!< in: pointer to 6 bytes */
	
/*******************************************************//**
The following function is used to store data in 7 consecutive
bytes. We store the most significant char to the lowest address. */
void mach_write_to_7(
	unsigned char*		b,	/*!< in: pointer to 7 bytes where to store */
	uint64	n);	/*!< in: 56-bit integer */
	
/********************************************************//**
The following function is used to fetch data from 7 consecutive
bytes. The most significant char is at the lowest address.
@return	56-bit integer */
uint64 mach_read_from_7(
	const unsigned char*	b);	/*!< in: pointer to 7 bytes */
	
/*******************************************************//**
The following function is used to store data in 8 consecutive
bytes. We store the most significant char to the lowest address. */
void mach_write_to_8(
	void*		b,	/*!< in: pointer to 8 bytes where to store */
	uint64	n);	/*!< in: 64-bit integer to be stored */
	
/********************************************************//**
The following function is used to fetch data from 8 consecutive
bytes. The most significant char is at the lowest address.
@return	64-bit integer */
uint64 mach_read_from_8(
	const unsigned char*	b);	/*!< in: pointer to 8 bytes */
	
/*********************************************************//**
Writes a 64-bit integer in a compressed form (5..9 bytes).
@return	size in bytes */
uint32 mach_ull_write_compressed(
	unsigned char*		b,	/*!< in: pointer to memory where to store */
	uint64	n);	/*!< in: 64-bit integer to be stored */
	
/*********************************************************//**
Returns the size of a 64-bit integer when written in the compressed form.
@return	compressed size in bytes */
uint32 mach_ull_get_compressed_size(
	uint64	n);	/*!< in: 64-bit integer to be stored */
	
/*********************************************************//**
Reads a 64-bit integer in a compressed form.
@return	the value read */
uint64 mach_ull_read_compressed(
	const unsigned char*	b);	/*!< in: pointer to memory from where to read */
	
/*********************************************************//**
Writes a 64-bit integer in a compressed form (1..11 bytes).
@return	size in bytes */
uint32 mach_ull_write_much_compressed(
	unsigned char*	b,	/*!< in: pointer to memory where to store */
	uint64	n);	/*!< in: 64-bit integer to be stored */
	
/*********************************************************//**
Returns the size of a 64-bit integer when written in the compressed form.
@return	compressed size in bytes */
uint32 mach_ull_get_much_compressed_size(
	uint64	n);	/*!< in: 64-bit integer to be stored */
	
/*********************************************************//**
Reads a 64-bit integer in a compressed form.
@return	the value read */
uint64 mach_ull_read_much_compressed(
	const unsigned char*	b);	/*!< in: pointer to memory from where to read */
	
/*********************************************************//**
Reads a uint32 in a compressed form if the log record fully contains it.
@return	pointer to end of the stored field, NULL if not complete */
unsigned char* mach_parse_compressed(
	unsigned char*	ptr,	/*!< in: pointer to buffer from where to read */
	unsigned char*	end_ptr,/*!< in: pointer to end of the buffer */
	uint32*	val);	/*!< out: read value */

/*********************************************************//**
Reads a 64-bit integer in a compressed form
if the log record fully contains it.
@return pointer to end of the stored field, NULL if not complete */
unsigned char* mach_ull_parse_compressed(
	unsigned char*		ptr,	/*!< in: pointer to buffer from where to read */
	unsigned char*		end_ptr,/*!< in: pointer to end of the buffer */
	uint64*	val);	/*!< out: read value */

/*********************************************************//**
Reads a double. It is stored in a little-endian format.
@return	double read */
double mach_double_read(
	const unsigned char*	b);	/*!< in: pointer to memory from where to read */
	
/*********************************************************//**
Writes a double. It is stored in a little-endian format. */
void mach_double_write(
	unsigned char*	b,	/*!< in: pointer to memory where to write */
	double	d);	/*!< in: double */

/*********************************************************//**
Reads a float. It is stored in a little-endian format.
@return	float read */
float mach_float_read(
	const unsigned char*	b);	/*!< in: pointer to memory from where to read */
	
/*********************************************************//**
Writes a float. It is stored in a little-endian format. */
void mach_float_write(
	unsigned char*	b,	/*!< in: pointer to memory where to write */
	float	d);	/*!< in: float */
	
/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return	unsigned long int */
uint32 mach_read_from_n_little_endian(
	const unsigned char*	buf,		/*!< in: from where to read */
	uint32		buf_size);	/*!< in: from how many bytes to read */
	
/*********************************************************//**
Writes a uint32 in the little-endian format. */
void mach_write_to_n_little_endian(
/*==========================*/
	unsigned char*	dest,		/*!< in: where to write */
	uint32	dest_size,	/*!< in: into how many bytes to write */
	uint32	n);		/*!< in: unsigned long int to write */
	
/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return	unsigned long int */
uint32 mach_read_from_2_little_endian(
	const unsigned char*	buf);		/*!< in: from where to read */
	
/*********************************************************//**
Writes a uint32 in the little-endian format. */
void mach_write_to_2_little_endian(
	unsigned char*	dest,		/*!< in: where to write */
	uint32	n);		/*!< in: unsigned long int to write */
	
uint64 ut_ull_create(
  uint32  high, /*!< in: high-order 32 bits */
  uint32  low);  /*!< in: low-order 32 bits */

#ifdef __cplusplus
}
#endif

#endif  /* _OS_MACH_H */

