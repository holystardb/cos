
#include "os_mach.h"

/*******************************************************//**
The following function is used to store data in one unsigned char. */
void mach_write_to_1(
	unsigned char*	b,	/*!< in: pointer to unsigned char where to store */
	uint32	n)	/*!< in: uint32 integer to be stored, >= 0, < 256 */
{
	b[0] = (unsigned char) n;
}

/********************************************************//**
The following function is used to fetch data from one unsigned char.
@return	uint32 integer, >= 0, < 256 */
uint32 mach_read_from_1(
	const unsigned char*	b)	/*!< in: pointer to unsigned char */
{
	return((uint32)(b[0]));
}

/*******************************************************//**
The following function is used to store data in two consecutive
bytes. We store the most significant unsigned char to the lowest address. */
void mach_write_to_2(
	unsigned char*	b,	/*!< in: pointer to two bytes where to store */
	uint32	n)	/*!< in: uint32 integer to be stored */
{
	b[0] = (unsigned char)(n >> 8);
	b[1] = (unsigned char)(n);
}

/********************************************************//**
The following function is used to fetch data from 2 consecutive
bytes. The most significant unsigned char is at the lowest address.
@return	uint32 integer */
uint32 mach_read_from_2(
	const unsigned char*	b)	/*!< in: pointer to 2 bytes */
{
	return(((uint32)(b[0]) << 8) | (uint32)(b[1]));
}

/*******************************************************//**
The following function is used to store data in 3 consecutive
bytes. We store the most significant unsigned char to the lowest address. */
void mach_write_to_3(
	unsigned char*	b,	/*!< in: pointer to 3 bytes where to store */
	uint32	n)	/*!< in: uint32 integer to be stored */
{
	b[0] = (unsigned char)(n >> 16);
	b[1] = (unsigned char)(n >> 8);
	b[2] = (unsigned char)(n);
}

/********************************************************//**
The following function is used to fetch data from 3 consecutive
bytes. The most significant unsigned char is at the lowest address.
@return	uint32 integer */

uint32 mach_read_from_3(
	const unsigned char*	b)	/*!< in: pointer to 3 bytes */
{
	return( ((uint32)(b[0]) << 16)
		| ((uint32)(b[1]) << 8)
		| (uint32)(b[2])
		);
}

/*******************************************************//**
The following function is used to store data in four consecutive
bytes. We store the most significant unsigned char to the lowest address. */
void mach_write_to_4(
	unsigned char*	b,	/*!< in: pointer to four bytes where to store */
	uint32	n)	/*!< in: uint32 integer to be stored */
{
	b[0] = (unsigned char)(n >> 24);
	b[1] = (unsigned char)(n >> 16);
	b[2] = (unsigned char)(n >> 8);
	b[3] = (unsigned char) n;
}

/********************************************************//**
The following function is used to fetch data from 4 consecutive
bytes. The most significant unsigned char is at the lowest address.
@return	uint32 integer */
uint32 mach_read_from_4(
	const unsigned char*	b)	/*!< in: pointer to four bytes */
{
	return( ((uint32)(b[0]) << 24)
		| ((uint32)(b[1]) << 16)
		| ((uint32)(b[2]) << 8)
		| (uint32)(b[3])
		);
}

/*********************************************************//**
Writes a uint32 in a compressed form where the first unsigned char codes the
length of the stored uint32. We look at the most significant bits of
the unsigned char. If the most significant bit is zero, it means 1-unsigned char storage,
else if the 2nd bit is 0, it means 2-unsigned char storage, else if 3rd is 0,
it means 3-unsigned char storage, else if 4th is 0, it means 4-unsigned char storage,
else the storage is 5-unsigned char.
@return	compressed size in bytes */
uint32 mach_write_compressed(
	unsigned char*	b,	/*!< in: pointer to memory where to store */
	uint32	n)	/*!< in: uint32 integer (< 2^32) to be stored */
{
	if (n < 0x80UL) {
		mach_write_to_1(b, n);
		return(1);
	} else if (n < 0x4000UL) {
		mach_write_to_2(b, n | 0x8000UL);
		return(2);
	} else if (n < 0x200000UL) {
		mach_write_to_3(b, n | 0xC00000UL);
		return(3);
	} else if (n < 0x10000000UL) {
		mach_write_to_4(b, n | 0xE0000000UL);
		return(4);
	} else {
		mach_write_to_1(b, 0xF0UL);
		mach_write_to_4(b + 1, n);
		return(5);
	}
}

/*********************************************************//**
Returns the size of a uint32 when written in the compressed form.
@return	compressed size in bytes */
uint32 mach_get_compressed_size(
	uint32	n)	/*!< in: uint32 integer (< 2^32) to be stored */
{
	if (n < 0x80UL) {
		return(1);
	} else if (n < 0x4000UL) {
		return(2);
	} else if (n < 0x200000UL) {
		return(3);
	} else if (n < 0x10000000UL) {
		return(4);
	} else {
		return(5);
	}
}

/*********************************************************//**
Reads a uint32 in a compressed form.
@return	read integer (< 2^32) */
uint32 mach_read_compressed(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	uint32	flag;
	flag = mach_read_from_1(b);
	if (flag < 0x80UL) {
		return(flag);
	} else if (flag < 0xC0UL) {
		return(mach_read_from_2(b) & 0x7FFFUL);
	} else if (flag < 0xE0UL) {
		return(mach_read_from_3(b) & 0x3FFFFFUL);
	} else if (flag < 0xF0UL) {
		return(mach_read_from_4(b) & 0x1FFFFFFFUL);
	} else {
		return(mach_read_from_4(b + 1));
	}
}

/*******************************************************//**
The following function is used to store data in 8 consecutive
bytes. We store the most significant unsigned char to the lowest address. */
void mach_write_to_8(
	void*		b,	/*!< in: pointer to 8 bytes where to store */
	uint64	n)	/*!< in: 64-bit integer to be stored */
{
	mach_write_to_4((unsigned char *)b, (uint32) (n >> 32));
	mach_write_to_4((unsigned char *)b + 4, (uint32) n);
}

/********************************************************//**
The following function is used to fetch data from 8 consecutive
bytes. The most significant unsigned char is at the lowest address.
@return	64-bit integer */
uint64 mach_read_from_8(

	const unsigned char*	b)	/*!< in: pointer to 8 bytes */
{
	uint64	ull;
	ull = ((uint64) mach_read_from_4(b)) << 32;
	ull |= (uint64) mach_read_from_4(b + 4);
	return(ull);
}

/*******************************************************//**
The following function is used to store data in 7 consecutive
bytes. We store the most significant unsigned char to the lowest address. */
void mach_write_to_7(
	unsigned char*		b,	/*!< in: pointer to 7 bytes where to store */
	uint64	n)	/*!< in: 56-bit integer */
{
	mach_write_to_3(b, (uint32) (n >> 32));
	mach_write_to_4(b + 3, (uint32) n);
}

/********************************************************//**
The following function is used to fetch data from 7 consecutive
bytes. The most significant unsigned char is at the lowest address.
@return	56-bit integer */
uint64 mach_read_from_7(
	const unsigned char*	b)	/*!< in: pointer to 7 bytes */
{
	return(ut_ull_create(mach_read_from_3(b), mach_read_from_4(b + 3)));
}

/*******************************************************//**
The following function is used to store data in 6 consecutive
bytes. We store the most significant unsigned char to the lowest address. */
void mach_write_to_6(
	unsigned char*		b,	/*!< in: pointer to 6 bytes where to store */
	uint64	n)	/*!< in: 48-bit integer */
{
	mach_write_to_2(b, (uint32) (n >> 32));
	mach_write_to_4(b + 2, (uint32) n);
}

/********************************************************//**
The following function is used to fetch data from 6 consecutive
bytes. The most significant unsigned char is at the lowest address.
@return	48-bit integer */
uint64 mach_read_from_6(
	const unsigned char*	b)	/*!< in: pointer to 6 bytes */
{
	return(ut_ull_create(mach_read_from_2(b), mach_read_from_4(b + 2)));
}

/*********************************************************//**
Writes a 64-bit integer in a compressed form (5..9 bytes).
@return	size in bytes */
uint32 mach_ull_write_compressed(
	unsigned char*		b,	/*!< in: pointer to memory where to store */
	uint64	n)	/*!< in: 64-bit integer to be stored */
{
	uint32	size;
	size = mach_write_compressed(b, (uint32) (n >> 32));
	mach_write_to_4(b + size, (uint32) n);
	return(size + 4);
}

/*********************************************************//**
Returns the size of a 64-bit integer when written in the compressed form.
@return	compressed size in bytes */
uint32 mach_ull_get_compressed_size(
	uint64	n)	/*!< in: 64-bit integer to be stored */
{
	return(4 + mach_get_compressed_size((uint32) (n >> 32)));
}

/*********************************************************//**
Reads a 64-bit integer in a compressed form.
@return	the value read */
uint64 mach_ull_read_compressed(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	uint64	n;
	uint32		size;
	n = (uint64) mach_read_compressed(b);
	size = mach_get_compressed_size((uint32) n);
	n <<= 32;
	n |= (uint64) mach_read_from_4(b + size);
	return(n);
}

/*********************************************************//**
Writes a 64-bit integer in a compressed form (1..11 bytes).
@return	size in bytes */
uint32 mach_ull_write_much_compressed(
	unsigned char*		b,	/*!< in: pointer to memory where to store */
	uint64	n)	/*!< in: 64-bit integer to be stored */
{
	uint32	size;
	if (!(n >> 32)) {
		return(mach_write_compressed(b, (uint32) n));
	}
	*b = (unsigned char)0xFF;
	size = 1 + mach_write_compressed(b + 1, (uint32) (n >> 32));
	size += mach_write_compressed(b + size, (uint32) n & 0xFFFFFFFF);
	return(size);
}

/*********************************************************//**
Returns the size of a 64-bit integer when written in the compressed form.
@return	compressed size in bytes */
uint32 mach_ull_get_much_compressed_size(
	uint64	n)	/*!< in: 64-bit integer to be stored */
{
	if (!(n >> 32)) {
		return(mach_get_compressed_size((uint32) n));
	}
	return(1 + mach_get_compressed_size((uint32) (n >> 32))
	       + mach_get_compressed_size((uint32) n & 0xFFFFFFFF));
}

/*********************************************************//**
Reads a 64-bit integer in a compressed form.
@return	the value read */
uint64 mach_ull_read_much_compressed(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	uint64	n;
	uint32		size;
	if (*b != (unsigned char)0xFF) {
		n = 0;
		size = 0;
	} else {
		n = (uint64) mach_read_compressed(b + 1);

		size = 1 + mach_get_compressed_size((uint32) n);
		n <<= 32;
	}
	n |= mach_read_compressed(b + size);
	return(n);
}

/*********************************************************//**
Reads a 64-bit integer in a compressed form
if the log record fully contains it.
@return pointer to end of the stored field, NULL if not complete */
unsigned char* mach_ull_parse_compressed(
/*======================*/
	unsigned char*		ptr,	/* in: pointer to buffer from where to read */
	unsigned char*		end_ptr,/* in: pointer to end of the buffer */
	uint64*	val)	/* out: read value */
{
	uint32		size;
	if (end_ptr < ptr + 5) {
		return(NULL);
	}
	*val = mach_read_compressed(ptr);
	size = mach_get_compressed_size((uint32) *val);
	ptr += size;
	if (end_ptr < ptr + 4) {
		return(NULL);
	}
	*val <<= 32;
	*val |= mach_read_from_4(ptr);
	return(ptr + 4);
}

/*********************************************************//**
Reads a double. It is stored in a little-endian format.
@return	double read */
double mach_double_read(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	double	d;
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(double); i++) {
#ifdef BIG_ENDIAN
		ptr[sizeof(double) - i - 1] = b[i];
#else
		ptr[i] = b[i];
#endif
	}
	return(d);
}

/*********************************************************//**
Writes a double. It is stored in a little-endian format. */
void mach_double_write(
/*==============*/
	unsigned char*	b,	/*!< in: pointer to memory where to write */
	double	d)	/*!< in: double */
{
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(double); i++) {
#ifdef BIG_ENDIAN
		b[i] = ptr[sizeof(double) - i - 1];
#else
		b[i] = ptr[i];
#endif
	}
}

/*********************************************************//**
Reads a float. It is stored in a little-endian format.
@return	float read */
float mach_float_read(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	float	d;
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(float); i++) {
#ifdef BIG_ENDIAN
		ptr[sizeof(float) - i - 1] = b[i];
#else
		ptr[i] = b[i];
#endif
	}
	return(d);
}

/*********************************************************//**
Writes a float. It is stored in a little-endian format. */
void mach_float_write(
	unsigned char*	b,	/*!< in: pointer to memory where to write */
	float	d)	/*!< in: float */
{
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(float); i++) {
#ifdef BIG_ENDIAN
		b[i] = ptr[sizeof(float) - i - 1];
#else
		b[i] = ptr[i];
#endif
	}
}

/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return	unsigned long int */
uint32 mach_read_from_n_little_endian(
/*===========================*/
	const unsigned char*	buf,		/*!< in: from where to read */
	uint32		buf_size)	/*!< in: from how many bytes to read */
{
	uint32	n	= 0;
	const unsigned char*	ptr;
	ptr = buf + buf_size;
	for (;;) {
		ptr--;
		n = n << 8;
		n += (uint32)(*ptr);
		if (ptr == buf) {
			break;
		}
	}
	return(n);
}

/*********************************************************//**
Writes a uint32 in the little-endian format. */
void mach_write_to_n_little_endian(
	unsigned char*	dest,		/*!< in: where to write */
	uint32	dest_size,	/*!< in: into how many bytes to write */
	uint32	n)		/*!< in: unsigned long int to write */
{
	unsigned char*	end;
	end = dest + dest_size;
	for (;;) {
		*dest = (unsigned char)(n & 0xFF);
		n = n >> 8;
		dest++;
		if (dest == end) {
			break;
		}
	}
}

/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return	unsigned long int */
uint32 mach_read_from_2_little_endian(
	const unsigned char*	buf)		/*!< in: from where to read */
{
	return((uint32)(buf[0]) | ((uint32)(buf[1]) << 8));
}

/*********************************************************//**
Writes a uint32 in the little-endian format. */
void mach_write_to_2_little_endian(
	unsigned char*	dest,		/*!< in: where to write */
	uint32	n)		/*!< in: unsigned long int to write */
{
	*dest = (unsigned char)(n & 0xFFUL);
	n = n >> 8;
	dest++;
	*dest = (unsigned char)(n & 0xFFUL);
}

/*********************************************************//**
Swap unsigned char ordering. */
void mach_swap_byte_order(
        unsigned char*           dest,           /*!< out: where to write */
        const unsigned char*     from,           /*!< in: where to read from */
        uint32           len)            /*!< in: length of src */
{
        dest += len;
        switch (len & 0x7) {
        case 0: *--dest = *from++;
        case 7: *--dest = *from++;
        case 6: *--dest = *from++;
        case 5: *--dest = *from++;
        case 4: *--dest = *from++;
        case 3: *--dest = *from++;
        case 2: *--dest = *from++;
        case 1: *--dest = *from;
        }
}

uint64 ut_ull_create(
	uint32	high,	/*!< in: high-order 32 bits */
	uint32	low)	/*!< in: low-order 32 bits */
{
	return(((uint64) high) << 32 | low);
}


