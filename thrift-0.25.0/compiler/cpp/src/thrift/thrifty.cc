/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 4 "thrift/thrifty.yy"

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Thrift parser.
 *
 * This parser is used on a thrift definition file.
 *
 */

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <stdio.h>
#include <string.h>
#ifndef _MSC_VER
#include <inttypes.h>
#else
#include <stdint.h>
#endif
#include <limits.h>
#ifdef _MSC_VER
#include "thrift/windows/config.h"
#endif
#include "thrift/main.h"
#include "thrift/common.h"
#include "thrift/globals.h"
#include "thrift/parse/t_program.h"
#include "thrift/parse/t_scope.h"

#ifdef _MSC_VER
//warning C4065: switch statement contains 'default' but no 'case' labels
#pragma warning(disable:4065)
#endif

/**
 * This global variable is used for automatic numbering of field indices etc.
 * when parsing the members of a struct. Field values are automatically
 * assigned starting from -1 and working their way down.
 */
int y_field_val = -1;
/**
 * This global variable is used for automatic numbering of enum values.
 * y_enum_val is the last value assigned; the next auto-assigned value will be
 * y_enum_val+1, and then it continues working upwards.  Explicitly specified
 * enum values reset y_enum_val to that value.
 */
int32_t y_enum_val = -1;
int g_arglist = 0;
const int struct_is_struct = 0;
const int struct_is_union = 1;


#line 145 "thrift/thrifty.cc"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
#ifndef YY_YY_THRIFT_THRIFTY_HH_INCLUDED
# define YY_YY_THRIFT_THRIFTY_HH_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif
/* "%code requires" blocks.  */
#line 1 "thrift/thrifty.yy"

#include "thrift/parse/t_program.h"

#line 184 "thrift/thrifty.cc"

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    tok_identifier = 258,          /* tok_identifier  */
    tok_literal = 259,             /* tok_literal  */
    tok_doctext = 260,             /* tok_doctext  */
    tok_int_constant = 261,        /* tok_int_constant  */
    tok_dub_constant = 262,        /* tok_dub_constant  */
    tok_include = 263,             /* tok_include  */
    tok_namespace = 264,           /* tok_namespace  */
    tok_cpp_include = 265,         /* tok_cpp_include  */
    tok_cpp_type = 266,            /* tok_cpp_type  */
    tok_xsd_all = 267,             /* tok_xsd_all  */
    tok_xsd_optional = 268,        /* tok_xsd_optional  */
    tok_xsd_nillable = 269,        /* tok_xsd_nillable  */
    tok_xsd_attrs = 270,           /* tok_xsd_attrs  */
    tok_void = 271,                /* tok_void  */
    tok_bool = 272,                /* tok_bool  */
    tok_string = 273,              /* tok_string  */
    tok_binary = 274,              /* tok_binary  */
    tok_uuid = 275,                /* tok_uuid  */
    tok_byte = 276,                /* tok_byte  */
    tok_i8 = 277,                  /* tok_i8  */
    tok_i16 = 278,                 /* tok_i16  */
    tok_i32 = 279,                 /* tok_i32  */
    tok_i64 = 280,                 /* tok_i64  */
    tok_double = 281,              /* tok_double  */
    tok_map = 282,                 /* tok_map  */
    tok_list = 283,                /* tok_list  */
    tok_set = 284,                 /* tok_set  */
    tok_oneway = 285,              /* tok_oneway  */
    tok_async = 286,               /* tok_async  */
    tok_typedef = 287,             /* tok_typedef  */
    tok_struct = 288,              /* tok_struct  */
    tok_xception = 289,            /* tok_xception  */
    tok_throws = 290,              /* tok_throws  */
    tok_extends = 291,             /* tok_extends  */
    tok_service = 292,             /* tok_service  */
    tok_enum = 293,                /* tok_enum  */
    tok_const = 294,               /* tok_const  */
    tok_required = 295,            /* tok_required  */
    tok_optional = 296,            /* tok_optional  */
    tok_union = 297,               /* tok_union  */
    tok_reference = 298            /* tok_reference  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif
/* Token kinds.  */
#define YYEMPTY -2
#define YYEOF 0
#define YYerror 256
#define YYUNDEF 257
#define tok_identifier 258
#define tok_literal 259
#define tok_doctext 260
#define tok_int_constant 261
#define tok_dub_constant 262
#define tok_include 263
#define tok_namespace 264
#define tok_cpp_include 265
#define tok_cpp_type 266
#define tok_xsd_all 267
#define tok_xsd_optional 268
#define tok_xsd_nillable 269
#define tok_xsd_attrs 270
#define tok_void 271
#define tok_bool 272
#define tok_string 273
#define tok_binary 274
#define tok_uuid 275
#define tok_byte 276
#define tok_i8 277
#define tok_i16 278
#define tok_i32 279
#define tok_i64 280
#define tok_double 281
#define tok_map 282
#define tok_list 283
#define tok_set 284
#define tok_oneway 285
#define tok_async 286
#define tok_typedef 287
#define tok_struct 288
#define tok_xception 289
#define tok_throws 290
#define tok_extends 291
#define tok_service 292
#define tok_enum 293
#define tok_const 294
#define tok_required 295
#define tok_optional 296
#define tok_union 297
#define tok_reference 298

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 82 "thrift/thrifty.yy"

  char*          id;
  int64_t        iconst;
  double         dconst;
  bool           tbool;
  t_doc*         tdoc;
  t_type*        ttype;
  t_base_type*   tbase;
  t_typedef*     ttypedef;
  t_enum*        tenum;
  t_enum_value*  tenumv;
  t_const*       tconst;
  t_const_value* tconstv;
  t_struct*      tstruct;
  t_service*     tservice;
  t_function*    tfunction;
  t_field*       tfield;
  char*          dtext;
  char*          keyword;
  t_field::e_req ereq;
  t_annotation*  tannot;
  t_field_id     tfieldid;

#line 314 "thrift/thrifty.cc"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;


int yyparse (void);


#endif /* !YY_YY_THRIFT_THRIFTY_HH_INCLUDED  */
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_tok_identifier = 3,             /* tok_identifier  */
  YYSYMBOL_tok_literal = 4,                /* tok_literal  */
  YYSYMBOL_tok_doctext = 5,                /* tok_doctext  */
  YYSYMBOL_tok_int_constant = 6,           /* tok_int_constant  */
  YYSYMBOL_tok_dub_constant = 7,           /* tok_dub_constant  */
  YYSYMBOL_tok_include = 8,                /* tok_include  */
  YYSYMBOL_tok_namespace = 9,              /* tok_namespace  */
  YYSYMBOL_tok_cpp_include = 10,           /* tok_cpp_include  */
  YYSYMBOL_tok_cpp_type = 11,              /* tok_cpp_type  */
  YYSYMBOL_tok_xsd_all = 12,               /* tok_xsd_all  */
  YYSYMBOL_tok_xsd_optional = 13,          /* tok_xsd_optional  */
  YYSYMBOL_tok_xsd_nillable = 14,          /* tok_xsd_nillable  */
  YYSYMBOL_tok_xsd_attrs = 15,             /* tok_xsd_attrs  */
  YYSYMBOL_tok_void = 16,                  /* tok_void  */
  YYSYMBOL_tok_bool = 17,                  /* tok_bool  */
  YYSYMBOL_tok_string = 18,                /* tok_string  */
  YYSYMBOL_tok_binary = 19,                /* tok_binary  */
  YYSYMBOL_tok_uuid = 20,                  /* tok_uuid  */
  YYSYMBOL_tok_byte = 21,                  /* tok_byte  */
  YYSYMBOL_tok_i8 = 22,                    /* tok_i8  */
  YYSYMBOL_tok_i16 = 23,                   /* tok_i16  */
  YYSYMBOL_tok_i32 = 24,                   /* tok_i32  */
  YYSYMBOL_tok_i64 = 25,                   /* tok_i64  */
  YYSYMBOL_tok_double = 26,                /* tok_double  */
  YYSYMBOL_tok_map = 27,                   /* tok_map  */
  YYSYMBOL_tok_list = 28,                  /* tok_list  */
  YYSYMBOL_tok_set = 29,                   /* tok_set  */
  YYSYMBOL_tok_oneway = 30,                /* tok_oneway  */
  YYSYMBOL_tok_async = 31,                 /* tok_async  */
  YYSYMBOL_tok_typedef = 32,               /* tok_typedef  */
  YYSYMBOL_tok_struct = 33,                /* tok_struct  */
  YYSYMBOL_tok_xception = 34,              /* tok_xception  */
  YYSYMBOL_tok_throws = 35,                /* tok_throws  */
  YYSYMBOL_tok_extends = 36,               /* tok_extends  */
  YYSYMBOL_tok_service = 37,               /* tok_service  */
  YYSYMBOL_tok_enum = 38,                  /* tok_enum  */
  YYSYMBOL_tok_const = 39,                 /* tok_const  */
  YYSYMBOL_tok_required = 40,              /* tok_required  */
  YYSYMBOL_tok_optional = 41,              /* tok_optional  */
  YYSYMBOL_tok_union = 42,                 /* tok_union  */
  YYSYMBOL_tok_reference = 43,             /* tok_reference  */
  YYSYMBOL_44_ = 44,                       /* '*'  */
  YYSYMBOL_45_ = 45,                       /* ','  */
  YYSYMBOL_46_ = 46,                       /* ';'  */
  YYSYMBOL_47_ = 47,                       /* '{'  */
  YYSYMBOL_48_ = 48,                       /* '}'  */
  YYSYMBOL_49_ = 49,                       /* '='  */
  YYSYMBOL_50_ = 50,                       /* '['  */
  YYSYMBOL_51_ = 51,                       /* ']'  */
  YYSYMBOL_52_ = 52,                       /* ':'  */
  YYSYMBOL_53_ = 53,                       /* '('  */
  YYSYMBOL_54_ = 54,                       /* ')'  */
  YYSYMBOL_55_ = 55,                       /* '<'  */
  YYSYMBOL_56_ = 56,                       /* '>'  */
  YYSYMBOL_YYACCEPT = 57,                  /* $accept  */
  YYSYMBOL_Program = 58,                   /* Program  */
  YYSYMBOL_CaptureDocText = 59,            /* CaptureDocText  */
  YYSYMBOL_DestroyDocText = 60,            /* DestroyDocText  */
  YYSYMBOL_HeaderList = 61,                /* HeaderList  */
  YYSYMBOL_Header = 62,                    /* Header  */
  YYSYMBOL_Include = 63,                   /* Include  */
  YYSYMBOL_DefinitionList = 64,            /* DefinitionList  */
  YYSYMBOL_Definition = 65,                /* Definition  */
  YYSYMBOL_TypeDefinition = 66,            /* TypeDefinition  */
  YYSYMBOL_CommaOrSemicolonOptional = 67,  /* CommaOrSemicolonOptional  */
  YYSYMBOL_Typedef = 68,                   /* Typedef  */
  YYSYMBOL_Enum = 69,                      /* Enum  */
  YYSYMBOL_EnumDefList = 70,               /* EnumDefList  */
  YYSYMBOL_EnumDef = 71,                   /* EnumDef  */
  YYSYMBOL_EnumValue = 72,                 /* EnumValue  */
  YYSYMBOL_Const = 73,                     /* Const  */
  YYSYMBOL_ConstValue = 74,                /* ConstValue  */
  YYSYMBOL_ConstList = 75,                 /* ConstList  */
  YYSYMBOL_ConstListContents = 76,         /* ConstListContents  */
  YYSYMBOL_ConstMap = 77,                  /* ConstMap  */
  YYSYMBOL_ConstMapContents = 78,          /* ConstMapContents  */
  YYSYMBOL_StructHead = 79,                /* StructHead  */
  YYSYMBOL_Struct = 80,                    /* Struct  */
  YYSYMBOL_XsdAll = 81,                    /* XsdAll  */
  YYSYMBOL_XsdOptional = 82,               /* XsdOptional  */
  YYSYMBOL_XsdNillable = 83,               /* XsdNillable  */
  YYSYMBOL_XsdAttributes = 84,             /* XsdAttributes  */
  YYSYMBOL_Xception = 85,                  /* Xception  */
  YYSYMBOL_Service = 86,                   /* Service  */
  YYSYMBOL_FlagArgs = 87,                  /* FlagArgs  */
  YYSYMBOL_UnflagArgs = 88,                /* UnflagArgs  */
  YYSYMBOL_Extends = 89,                   /* Extends  */
  YYSYMBOL_FunctionList = 90,              /* FunctionList  */
  YYSYMBOL_Function = 91,                  /* Function  */
  YYSYMBOL_Oneway = 92,                    /* Oneway  */
  YYSYMBOL_Throws = 93,                    /* Throws  */
  YYSYMBOL_FieldList = 94,                 /* FieldList  */
  YYSYMBOL_Field = 95,                     /* Field  */
  YYSYMBOL_FieldName = 96,                 /* FieldName  */
  YYSYMBOL_FieldIdentifier = 97,           /* FieldIdentifier  */
  YYSYMBOL_FieldReference = 98,            /* FieldReference  */
  YYSYMBOL_FieldRequiredness = 99,         /* FieldRequiredness  */
  YYSYMBOL_FieldValue = 100,               /* FieldValue  */
  YYSYMBOL_FunctionType = 101,             /* FunctionType  */
  YYSYMBOL_FieldType = 102,                /* FieldType  */
  YYSYMBOL_BaseType = 103,                 /* BaseType  */
  YYSYMBOL_SimpleBaseType = 104,           /* SimpleBaseType  */
  YYSYMBOL_ContainerType = 105,            /* ContainerType  */
  YYSYMBOL_SimpleContainerType = 106,      /* SimpleContainerType  */
  YYSYMBOL_MapType = 107,                  /* MapType  */
  YYSYMBOL_SetType = 108,                  /* SetType  */
  YYSYMBOL_ListType = 109,                 /* ListType  */
  YYSYMBOL_CppType = 110,                  /* CppType  */
  YYSYMBOL_TypeAnnotations = 111,          /* TypeAnnotations  */
  YYSYMBOL_TypeAnnotationList = 112,       /* TypeAnnotationList  */
  YYSYMBOL_TypeAnnotation = 113,           /* TypeAnnotation  */
  YYSYMBOL_TypeAnnotationValue = 114       /* TypeAnnotationValue  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if !defined yyoverflow

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  3
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   215

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  57
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  58
/* YYNRULES -- Number of rules.  */
#define YYNRULES  143
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  224

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   298


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      53,    54,    44,     2,    45,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    52,    46,
      55,    49,    56,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    50,     2,    51,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    47,     2,    48,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   249,   249,   261,   272,   281,   286,   291,   295,   307,
     315,   325,   338,   346,   351,   359,   374,   392,   399,   406,
     413,   422,   424,   427,   430,   443,   471,   478,   485,   499,
     514,   526,   545,   554,   560,   565,   571,   576,   583,   590,
     597,   604,   611,   618,   625,   629,   635,   650,   655,   660,
     665,   670,   675,   680,   685,   690,   704,   718,   723,   728,
     741,   746,   753,   759,   774,   778,   783,   788,   798,   803,
     813,   820,   854,   859,   864,   876,   881,   886,   891,   896,
     901,   906,   911,   916,   921,   926,   931,   936,   941,   946,
     951,   956,   961,   966,   971,   976,   981,   986,   991,   996,
    1001,  1006,  1011,  1019,  1059,  1069,  1074,  1079,  1083,  1095,
    1100,  1109,  1114,  1119,  1126,  1145,  1150,  1156,  1169,  1174,
    1179,  1184,  1189,  1194,  1199,  1204,  1209,  1214,  1220,  1231,
    1236,  1241,  1248,  1258,  1268,  1286,  1291,  1296,  1302,  1307,
    1315,  1321,  1330,  1336
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "tok_identifier",
  "tok_literal", "tok_doctext", "tok_int_constant", "tok_dub_constant",
  "tok_include", "tok_namespace", "tok_cpp_include", "tok_cpp_type",
  "tok_xsd_all", "tok_xsd_optional", "tok_xsd_nillable", "tok_xsd_attrs",
  "tok_void", "tok_bool", "tok_string", "tok_binary", "tok_uuid",
  "tok_byte", "tok_i8", "tok_i16", "tok_i32", "tok_i64", "tok_double",
  "tok_map", "tok_list", "tok_set", "tok_oneway", "tok_async",
  "tok_typedef", "tok_struct", "tok_xception", "tok_throws", "tok_extends",
  "tok_service", "tok_enum", "tok_const", "tok_required", "tok_optional",
  "tok_union", "tok_reference", "'*'", "','", "';'", "'{'", "'}'", "'='",
  "'['", "']'", "':'", "'('", "')'", "'<'", "'>'", "$accept", "Program",
  "CaptureDocText", "DestroyDocText", "HeaderList", "Header", "Include",
  "DefinitionList", "Definition", "TypeDefinition",
  "CommaOrSemicolonOptional", "Typedef", "Enum", "EnumDefList", "EnumDef",
  "EnumValue", "Const", "ConstValue", "ConstList", "ConstListContents",
  "ConstMap", "ConstMapContents", "StructHead", "Struct", "XsdAll",
  "XsdOptional", "XsdNillable", "XsdAttributes", "Xception", "Service",
  "FlagArgs", "UnflagArgs", "Extends", "FunctionList", "Function",
  "Oneway", "Throws", "FieldList", "Field", "FieldName", "FieldIdentifier",
  "FieldReference", "FieldRequiredness", "FieldValue", "FunctionType",
  "FieldType", "BaseType", "SimpleBaseType", "ContainerType",
  "SimpleContainerType", "MapType", "SetType", "ListType", "CppType",
  "TypeAnnotations", "TypeAnnotationList", "TypeAnnotation",
  "TypeAnnotationValue", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-123)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-59)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
    -123,    11,     6,  -123,    28,    18,    17,     9,    22,  -123,
    -123,    50,  -123,    26,    29,  -123,   186,  -123,    36,    40,
      44,   186,  -123,  -123,  -123,  -123,  -123,  -123,    48,  -123,
    -123,  -123,     1,  -123,  -123,  -123,  -123,  -123,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,    45,    45,    45,    52,  -123,
       1,  -123,     1,  -123,  -123,  -123,    10,    34,    25,    61,
      53,  -123,  -123,    69,    20,    24,    31,     1,  -123,  -123,
    -123,    87,    46,  -123,    42,  -123,    47,     4,  -123,   186,
     186,   186,    -4,    51,  -123,  -123,    54,    27,  -123,    49,
    -123,  -123,    55,    39,    57,  -123,  -123,  -123,     1,    90,
    -123,  -123,     1,    98,  -123,  -123,  -123,  -123,  -123,  -123,
    -123,    -4,  -123,  -123,    58,    99,    -4,   186,    45,  -123,
    -123,    56,     5,    59,  -123,    60,     1,    21,    16,  -123,
       1,  -123,  -123,    63,  -123,  -123,  -123,  -123,   186,    19,
      62,  -123,   105,    -4,  -123,    64,  -123,    -4,  -123,  -123,
      72,  -123,  -123,   159,     1,  -123,  -123,    27,  -123,  -123,
     118,  -123,   102,  -123,  -123,    -4,  -123,  -123,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,  -123,  -123,    71,    70,  -123,
      27,   111,  -123,  -123,  -123,   116,    78,  -123,   110,   129,
     119,     1,   112,     1,  -123,    -4,  -123,    -4,   120,  -123,
     113,  -123,  -123,  -123
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       6,     0,    13,     1,     0,     3,     0,     0,     0,     5,
       7,     0,    11,     0,     0,    10,     0,    44,     0,     0,
       0,     0,    45,    12,    15,    17,    18,    14,     0,    19,
      20,    16,   138,     9,   114,   121,   118,   119,   120,   122,
     123,   124,   125,   126,   127,   136,   136,   136,     0,   115,
     138,   116,   138,   129,   130,   131,     0,    60,     0,     0,
      48,   140,     8,     0,     0,     0,     0,   138,   117,   128,
      70,     0,     0,    27,     0,    47,     0,     0,   135,     0,
       0,     0,    23,     3,    59,    57,     3,     0,    70,   143,
     137,   139,     0,     0,     0,    21,    22,    24,   138,   104,
      69,    62,   138,     0,    26,    35,    34,    32,    33,    43,
      40,    23,    36,    37,     3,     0,    23,     0,   136,   133,
      55,     0,   109,     3,    25,    30,   138,     0,     0,    31,
     138,   142,   141,     0,   134,   103,   107,   108,     0,    66,
       0,    61,     0,    23,    41,     0,    38,    23,    46,   132,
     106,    64,    65,     0,   138,    29,    28,     0,    39,   105,
       0,   113,     0,   112,    56,    23,    72,    75,    73,    74,
      76,    77,    84,    85,    86,    78,    79,    80,    81,    82,
      83,    87,    88,    89,    90,    91,    92,    93,    95,    97,
      96,    98,    99,   100,   101,   102,    94,   111,     0,    42,
       0,    50,    70,   110,    49,    52,     3,    51,    54,    68,
       0,   138,     0,   138,    70,    23,    70,    23,     3,    71,
       3,    63,    53,    67
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
    -123,  -123,    -1,  -123,  -123,  -123,  -123,  -123,  -123,  -123,
    -103,  -123,  -123,  -123,  -123,  -123,  -123,  -122,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,  -123,  -123,   -85,  -123,  -123,
    -123,  -123,  -123,  -123,  -123,   -20,  -123,  -123,  -123,  -123,
    -123,  -123,  -123,   -37,   -50,  -123,  -123,  -123
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,     1,    99,     4,     2,     9,    10,     5,    23,    24,
      97,    25,    26,    86,   104,   126,    27,   111,   112,   128,
     113,   127,    28,    29,    76,   205,   208,   211,    30,    31,
     101,   140,    72,   123,   141,   153,   213,    83,   100,   197,
     122,   160,   138,   201,   162,    48,    49,    50,    51,    52,
      53,    54,    55,    64,    62,    77,    91,   116
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      68,    59,    69,   114,    11,   145,   147,    89,   129,    65,
      66,     3,    13,   132,    -4,    -4,    -4,    82,    -2,   105,
     106,    12,   107,   108,   105,   106,    15,   107,   108,    32,
     105,   106,    33,   107,   108,   165,     6,     7,     8,    56,
     156,    95,    96,    57,   158,   136,   137,    58,   120,   151,
     152,    60,   124,    14,    61,    67,    63,    70,    90,    92,
      93,    94,   199,   109,    74,    75,   110,   146,   109,   144,
      71,   110,    73,    78,   109,    79,   143,   110,   203,    80,
     148,   134,    16,    17,    18,   103,    81,    19,    20,    21,
      84,    87,    22,    85,    88,   118,   121,   133,   115,    98,
     117,   125,   102,   131,   164,   198,   130,   -58,   135,   142,
     154,   155,   219,   119,   221,   159,   157,   206,   150,   149,
     200,   166,   139,   202,   204,   210,   167,   168,   169,   218,
     207,   220,   209,   163,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   215,    34,   217,   212,   216,   214,   223,   222,     0,
       0,     0,     0,     0,     0,   161,    35,    36,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    47,    34,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,    45,    46,    47
};

static const yytype_int16 yycheck[] =
{
      50,    21,    52,    88,     5,   127,   128,     3,   111,    46,
      47,     0,     3,   116,     8,     9,    10,    67,     0,     3,
       4,     4,     6,     7,     3,     4,     4,     6,     7,     3,
       3,     4,     3,     6,     7,   157,     8,     9,    10,     3,
     143,    45,    46,     3,   147,    40,    41,     3,    98,    30,
      31,     3,   102,    44,    53,     3,    11,    47,    54,    79,
      80,    81,   165,    47,     3,    12,    50,    51,    47,    48,
      36,    50,    47,     4,    47,    55,   126,    50,   200,    55,
     130,   118,    32,    33,    34,    86,    55,    37,    38,    39,
       3,    49,    42,    47,    47,    56,     6,   117,    49,    48,
      45,     3,    48,     4,   154,     3,    48,    48,    52,    49,
      48,     6,   215,    56,   217,    43,    52,   202,   138,    56,
      49,     3,   123,    53,    13,    15,     8,     9,    10,   214,
      14,   216,    54,   153,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    40,    41,
      42,   211,     3,   213,    35,    53,    47,    54,    48,    -1,
      -1,    -1,    -1,    -1,    -1,    16,    17,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,     3,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,    58,    61,     0,    60,    64,     8,     9,    10,    62,
      63,    59,     4,     3,    44,     4,    32,    33,    34,    37,
      38,    39,    42,    65,    66,    68,    69,    73,    79,    80,
      85,    86,     3,     3,     3,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,   102,   103,
     104,   105,   106,   107,   108,   109,     3,     3,     3,   102,
       3,    53,   111,    11,   110,   110,   110,     3,   111,   111,
      47,    36,    89,    47,     3,    12,    81,   112,     4,    55,
      55,    55,   111,    94,     3,    47,    70,    49,    47,     3,
      54,   113,   102,   102,   102,    45,    46,    67,    48,    59,
      95,    87,    48,    59,    71,     3,     4,     6,     7,    47,
      50,    74,    75,    77,    94,    49,   114,    45,    56,    56,
     111,     6,    97,    90,   111,     3,    72,    78,    76,    67,
      48,     4,    67,   102,   110,    52,    40,    41,    99,    59,
      88,    91,    49,   111,    48,    74,    51,    74,   111,    56,
     102,    30,    31,    92,    48,     6,    67,    52,    67,    43,
      98,    16,   101,   102,   111,    74,     3,     8,     9,    10,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    39,    40,    41,    42,    96,     3,    67,
      49,   100,    53,    74,    13,    82,    94,    14,    83,    54,
      15,    84,    35,    93,    47,   111,    53,   111,    94,    67,
      94,    67,    48,    54
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    57,    58,    59,    60,    61,    61,    62,    62,    62,
      62,    63,    64,    64,    65,    65,    65,    66,    66,    66,
      66,    67,    67,    67,    68,    69,    70,    70,    71,    72,
      72,    73,    74,    74,    74,    74,    74,    74,    75,    76,
      76,    77,    78,    78,    79,    79,    80,    81,    81,    82,
      82,    83,    83,    84,    84,    85,    86,    87,    88,    89,
      89,    90,    90,    91,    92,    92,    92,    93,    93,    94,
      94,    95,    96,    96,    96,    96,    96,    96,    96,    96,
      96,    96,    96,    96,    96,    96,    96,    96,    96,    96,
      96,    96,    96,    96,    96,    96,    96,    96,    96,    96,
      96,    96,    96,    97,    97,    98,    98,    99,    99,    99,
     100,   100,   101,   101,   102,   102,   102,   103,   104,   104,
     104,   104,   104,   104,   104,   104,   104,   104,   105,   106,
     106,   106,   107,   108,   109,   110,   110,   111,   111,   112,
     112,   113,   114,   114
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     0,     0,     3,     0,     1,     4,     3,
       2,     2,     3,     0,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     0,     5,     6,     2,     0,     4,     3,
       1,     6,     1,     1,     1,     1,     1,     1,     3,     3,
       0,     3,     5,     0,     1,     1,     7,     1,     0,     1,
       0,     1,     0,     4,     0,     6,     9,     0,     0,     2,
       0,     2,     0,    10,     1,     1,     0,     4,     0,     2,
       0,    12,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     0,     1,     0,     1,     1,     0,
       2,     0,     1,     1,     1,     1,     1,     2,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     2,     1,
       1,     1,     7,     5,     6,     2,     0,     3,     0,     2,
       0,     3,     2,     0
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)]);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep)
{
  YY_USE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex ();
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* Program: HeaderList DefinitionList  */
#line 250 "thrift/thrifty.yy"
    {
      pdebug("Program -> Headers DefinitionList");
      if((g_program_doctext_candidate != nullptr) && (g_program_doctext_status != ALREADY_PROCESSED))
      {
        g_program->set_doc(g_program_doctext_candidate);
        g_program_doctext_status = ALREADY_PROCESSED;
      }
      clear_doctext();
    }
#line 1587 "thrift/thrifty.cc"
    break;

  case 3: /* CaptureDocText: %empty  */
#line 261 "thrift/thrifty.yy"
    {
      if (g_parse_mode == PROGRAM) {
        (yyval.dtext) = g_doctext;
        g_doctext = nullptr;
      } else {
        (yyval.dtext) = nullptr;
      }
    }
#line 1600 "thrift/thrifty.cc"
    break;

  case 4: /* DestroyDocText: %empty  */
#line 272 "thrift/thrifty.yy"
    {
      if (g_parse_mode == PROGRAM) {
        clear_doctext();
      }
    }
#line 1610 "thrift/thrifty.cc"
    break;

  case 5: /* HeaderList: HeaderList DestroyDocText Header  */
#line 282 "thrift/thrifty.yy"
    {
      pdebug("HeaderList -> HeaderList Header");
    }
#line 1618 "thrift/thrifty.cc"
    break;

  case 6: /* HeaderList: %empty  */
#line 286 "thrift/thrifty.yy"
    {
      pdebug("HeaderList -> ");
    }
#line 1626 "thrift/thrifty.cc"
    break;

  case 7: /* Header: Include  */
#line 292 "thrift/thrifty.yy"
    {
      pdebug("Header -> Include");
    }
#line 1634 "thrift/thrifty.cc"
    break;

  case 8: /* Header: tok_namespace tok_identifier tok_identifier TypeAnnotations  */
#line 296 "thrift/thrifty.yy"
    {
      pdebug("Header -> tok_namespace tok_identifier tok_identifier");
      declare_valid_program_doctext();
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace((yyvsp[-2].id), (yyvsp[-1].id));
      }
      if ((yyvsp[0].ttype) != nullptr) {
        g_program->set_namespace_annotations((yyvsp[-2].id), (yyvsp[0].ttype)->annotations_);
        delete (yyvsp[0].ttype);
      }
    }
#line 1650 "thrift/thrifty.cc"
    break;

  case 9: /* Header: tok_namespace '*' tok_identifier  */
#line 308 "thrift/thrifty.yy"
    {
      pdebug("Header -> tok_namespace * tok_identifier");
      declare_valid_program_doctext();
      if (g_parse_mode == PROGRAM) {
        g_program->set_namespace("*", (yyvsp[0].id));
      }
    }
#line 1662 "thrift/thrifty.cc"
    break;

  case 10: /* Header: tok_cpp_include tok_literal  */
#line 316 "thrift/thrifty.yy"
    {
      pdebug("Header -> tok_cpp_include tok_literal");
      declare_valid_program_doctext();
      if (g_parse_mode == PROGRAM) {
        g_program->add_cpp_include((yyvsp[0].id));
      }
    }
#line 1674 "thrift/thrifty.cc"
    break;

  case 11: /* Include: tok_include tok_literal  */
#line 326 "thrift/thrifty.yy"
    {
      pdebug("Include -> tok_include tok_literal");
      declare_valid_program_doctext();
      if (g_parse_mode == INCLUDES) {
        std::string path = include_file(std::string((yyvsp[0].id)));
        if (!path.empty()) {
          g_program->add_include(path, std::string((yyvsp[0].id)));
        }
      }
    }
#line 1689 "thrift/thrifty.cc"
    break;

  case 12: /* DefinitionList: DefinitionList CaptureDocText Definition  */
#line 339 "thrift/thrifty.yy"
    {
      pdebug("DefinitionList -> DefinitionList Definition");
      if ((yyvsp[-1].dtext) != nullptr && (yyvsp[0].tdoc) != nullptr) {
        (yyvsp[0].tdoc)->set_doc((yyvsp[-1].dtext));
      }
    }
#line 1700 "thrift/thrifty.cc"
    break;

  case 13: /* DefinitionList: %empty  */
#line 346 "thrift/thrifty.yy"
    {
      pdebug("DefinitionList -> ");
    }
#line 1708 "thrift/thrifty.cc"
    break;

  case 14: /* Definition: Const  */
#line 352 "thrift/thrifty.yy"
    {
      pdebug("Definition -> Const");
      if (g_parse_mode == PROGRAM) {
        g_program->add_const((yyvsp[0].tconst));
      }
      (yyval.tdoc) = (yyvsp[0].tconst);
    }
#line 1720 "thrift/thrifty.cc"
    break;

  case 15: /* Definition: TypeDefinition  */
#line 360 "thrift/thrifty.yy"
    {
      pdebug("Definition -> TypeDefinition");
      if (g_parse_mode == PROGRAM) {
        g_scope->add_type((yyvsp[0].ttype)->get_name(), (yyvsp[0].ttype));
        if (g_parent_scope != nullptr) {
          g_parent_scope->add_type(g_parent_prefix + (yyvsp[0].ttype)->get_name(), (yyvsp[0].ttype));
        }
        if (! g_program->is_unique_typename((yyvsp[0].ttype))) {
          yyerror("Type \"%s\" is already defined.", (yyvsp[0].ttype)->get_name().c_str());
          exit(1);
        }
      }
      (yyval.tdoc) = (yyvsp[0].ttype);
    }
#line 1739 "thrift/thrifty.cc"
    break;

  case 16: /* Definition: Service  */
#line 375 "thrift/thrifty.yy"
    {
      pdebug("Definition -> Service");
      if (g_parse_mode == PROGRAM) {
        g_scope->add_service((yyvsp[0].tservice)->get_name(), (yyvsp[0].tservice));
        if (g_parent_scope != nullptr) {
          g_parent_scope->add_service(g_parent_prefix + (yyvsp[0].tservice)->get_name(), (yyvsp[0].tservice));
        }
        g_program->add_service((yyvsp[0].tservice));
        if (! g_program->is_unique_typename((yyvsp[0].tservice))) {
          yyerror("Type \"%s\" is already defined.", (yyvsp[0].tservice)->get_name().c_str());
          exit(1);
        }
      }
      (yyval.tdoc) = (yyvsp[0].tservice);
    }
#line 1759 "thrift/thrifty.cc"
    break;

  case 17: /* TypeDefinition: Typedef  */
#line 393 "thrift/thrifty.yy"
    {
      pdebug("TypeDefinition -> Typedef");
      if (g_parse_mode == PROGRAM) {
        g_program->add_typedef((yyvsp[0].ttypedef));
      }
    }
#line 1770 "thrift/thrifty.cc"
    break;

  case 18: /* TypeDefinition: Enum  */
#line 400 "thrift/thrifty.yy"
    {
      pdebug("TypeDefinition -> Enum");
      if (g_parse_mode == PROGRAM) {
        g_program->add_enum((yyvsp[0].tenum));
      }
    }
#line 1781 "thrift/thrifty.cc"
    break;

  case 19: /* TypeDefinition: Struct  */
#line 407 "thrift/thrifty.yy"
    {
      pdebug("TypeDefinition -> Struct");
      if (g_parse_mode == PROGRAM) {
        g_program->add_struct((yyvsp[0].tstruct));
      }
    }
#line 1792 "thrift/thrifty.cc"
    break;

  case 20: /* TypeDefinition: Xception  */
#line 414 "thrift/thrifty.yy"
    {
      pdebug("TypeDefinition -> Xception");
      if (g_parse_mode == PROGRAM) {
        g_program->add_xception((yyvsp[0].tstruct));
      }
    }
#line 1803 "thrift/thrifty.cc"
    break;

  case 21: /* CommaOrSemicolonOptional: ','  */
#line 423 "thrift/thrifty.yy"
    {}
#line 1809 "thrift/thrifty.cc"
    break;

  case 22: /* CommaOrSemicolonOptional: ';'  */
#line 425 "thrift/thrifty.yy"
    {}
#line 1815 "thrift/thrifty.cc"
    break;

  case 23: /* CommaOrSemicolonOptional: %empty  */
#line 427 "thrift/thrifty.yy"
    {}
#line 1821 "thrift/thrifty.cc"
    break;

  case 24: /* Typedef: tok_typedef FieldType tok_identifier TypeAnnotations CommaOrSemicolonOptional  */
#line 431 "thrift/thrifty.yy"
    {
      pdebug("TypeDef -> tok_typedef FieldType tok_identifier");
      validate_simple_identifier( (yyvsp[-2].id));
      t_typedef *td = new t_typedef(g_program, (yyvsp[-3].ttype), (yyvsp[-2].id));
      (yyval.ttypedef) = td;
      if ((yyvsp[-1].ttype) != nullptr) {
        (yyval.ttypedef)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 1836 "thrift/thrifty.cc"
    break;

  case 25: /* Enum: tok_enum tok_identifier '{' EnumDefList '}' TypeAnnotations  */
#line 444 "thrift/thrifty.yy"
    {
      pdebug("Enum -> tok_enum tok_identifier { EnumDefList }");
      (yyval.tenum) = (yyvsp[-2].tenum);
      validate_simple_identifier( (yyvsp[-4].id));
      (yyval.tenum)->set_name((yyvsp[-4].id));
      if ((yyvsp[0].ttype) != nullptr) {
        (yyval.tenum)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }

      // make constants for all the enum values
      if (g_parse_mode == PROGRAM) {
        const std::vector<t_enum_value*>& enum_values = (yyval.tenum)->get_constants();
        std::vector<t_enum_value*>::const_iterator c_iter;
        for (c_iter = enum_values.begin(); c_iter != enum_values.end(); ++c_iter) {
          std::string const_name = (yyval.tenum)->get_name() + "." + (*c_iter)->get_name();
          t_const_value* const_val = new t_const_value((*c_iter)->get_value());
          const_val->set_enum((yyval.tenum));
          g_scope->add_constant(const_name, new t_const(g_type_i32, (*c_iter)->get_name(), const_val));
          if (g_parent_scope != nullptr) {
            g_parent_scope->add_constant(g_parent_prefix + const_name, new t_const(g_type_i32, (*c_iter)->get_name(), const_val));
          }
        }
      }
    }
#line 1866 "thrift/thrifty.cc"
    break;

  case 26: /* EnumDefList: EnumDefList EnumDef  */
#line 472 "thrift/thrifty.yy"
    {
      pdebug("EnumDefList -> EnumDefList EnumDef");
      (yyval.tenum) = (yyvsp[-1].tenum);
      (yyval.tenum)->append((yyvsp[0].tenumv));
    }
#line 1876 "thrift/thrifty.cc"
    break;

  case 27: /* EnumDefList: %empty  */
#line 478 "thrift/thrifty.yy"
    {
      pdebug("EnumDefList -> ");
      (yyval.tenum) = new t_enum(g_program);
      y_enum_val = -1;
    }
#line 1886 "thrift/thrifty.cc"
    break;

  case 28: /* EnumDef: CaptureDocText EnumValue TypeAnnotations CommaOrSemicolonOptional  */
#line 486 "thrift/thrifty.yy"
    {
      pdebug("EnumDef -> EnumValue");
      (yyval.tenumv) = (yyvsp[-2].tenumv);
      if ((yyvsp[-3].dtext) != nullptr) {
        (yyval.tenumv)->set_doc((yyvsp[-3].dtext));
      }
	  if ((yyvsp[-1].ttype) != nullptr) {
        (yyval.tenumv)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 1902 "thrift/thrifty.cc"
    break;

  case 29: /* EnumValue: tok_identifier '=' tok_int_constant  */
#line 500 "thrift/thrifty.yy"
    {
      pdebug("EnumValue -> tok_identifier = tok_int_constant");
      if ((yyvsp[0].iconst) < INT32_MIN || (yyvsp[0].iconst) > INT32_MAX) {
        // Note: this used to be just a warning.  However, since thrift always
        // treats enums as i32 values, I'm changing it to a fatal error.
        // I doubt this will affect many people, but users who run into this
        // will have to update their thrift files to manually specify the
        // truncated i32 value that thrift has always been using anyway.
        failure("64-bit value supplied for enum %s will be truncated.", (yyvsp[-2].id));
      }
      y_enum_val = static_cast<int32_t>((yyvsp[0].iconst));
      (yyval.tenumv) = new t_enum_value((yyvsp[-2].id), y_enum_val);
    }
#line 1920 "thrift/thrifty.cc"
    break;

  case 30: /* EnumValue: tok_identifier  */
#line 515 "thrift/thrifty.yy"
    {
      pdebug("EnumValue -> tok_identifier");
      validate_simple_identifier( (yyvsp[0].id));
      if (y_enum_val == INT32_MAX) {
        failure("enum value overflow at enum %s", (yyvsp[0].id));
      }
      ++y_enum_val;
      (yyval.tenumv) = new t_enum_value((yyvsp[0].id), y_enum_val);
    }
#line 1934 "thrift/thrifty.cc"
    break;

  case 31: /* Const: tok_const FieldType tok_identifier '=' ConstValue CommaOrSemicolonOptional  */
#line 527 "thrift/thrifty.yy"
    {
      pdebug("Const -> tok_const FieldType tok_identifier = ConstValue");
      if (g_parse_mode == PROGRAM) {
        validate_simple_identifier( (yyvsp[-3].id));
        g_scope->resolve_const_value((yyvsp[-1].tconstv), (yyvsp[-4].ttype));
        (yyval.tconst) = new t_const((yyvsp[-4].ttype), (yyvsp[-3].id), (yyvsp[-1].tconstv));
        validate_const_type((yyval.tconst));

        g_scope->add_constant((yyvsp[-3].id), (yyval.tconst));
        if (g_parent_scope != nullptr) {
          g_parent_scope->add_constant(g_parent_prefix + (yyvsp[-3].id), (yyval.tconst));
        }
      } else {
        (yyval.tconst) = nullptr;
      }
    }
#line 1955 "thrift/thrifty.cc"
    break;

  case 32: /* ConstValue: tok_int_constant  */
#line 546 "thrift/thrifty.yy"
    {
      pdebug("ConstValue => tok_int_constant");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_integer((yyvsp[0].iconst));
      if (!g_allow_64bit_consts && ((yyvsp[0].iconst) < INT32_MIN || (yyvsp[0].iconst) > INT32_MAX)) {
        pwarning(1, "64-bit constant \"%" PRIi64"\" may not work in all languages.\n", (yyvsp[0].iconst));
      }
    }
#line 1968 "thrift/thrifty.cc"
    break;

  case 33: /* ConstValue: tok_dub_constant  */
#line 555 "thrift/thrifty.yy"
    {
      pdebug("ConstValue => tok_dub_constant");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_double((yyvsp[0].dconst));
    }
#line 1978 "thrift/thrifty.cc"
    break;

  case 34: /* ConstValue: tok_literal  */
#line 561 "thrift/thrifty.yy"
    {
      pdebug("ConstValue => tok_literal");
      (yyval.tconstv) = new t_const_value((yyvsp[0].id));
    }
#line 1987 "thrift/thrifty.cc"
    break;

  case 35: /* ConstValue: tok_identifier  */
#line 566 "thrift/thrifty.yy"
    {
      pdebug("ConstValue => tok_identifier");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_identifier((yyvsp[0].id));
    }
#line 1997 "thrift/thrifty.cc"
    break;

  case 36: /* ConstValue: ConstList  */
#line 572 "thrift/thrifty.yy"
    {
      pdebug("ConstValue => ConstList");
      (yyval.tconstv) = (yyvsp[0].tconstv);
    }
#line 2006 "thrift/thrifty.cc"
    break;

  case 37: /* ConstValue: ConstMap  */
#line 577 "thrift/thrifty.yy"
    {
      pdebug("ConstValue => ConstMap");
      (yyval.tconstv) = (yyvsp[0].tconstv);
    }
#line 2015 "thrift/thrifty.cc"
    break;

  case 38: /* ConstList: '[' ConstListContents ']'  */
#line 584 "thrift/thrifty.yy"
    {
      pdebug("ConstList => [ ConstListContents ]");
      (yyval.tconstv) = (yyvsp[-1].tconstv);
    }
#line 2024 "thrift/thrifty.cc"
    break;

  case 39: /* ConstListContents: ConstListContents ConstValue CommaOrSemicolonOptional  */
#line 591 "thrift/thrifty.yy"
    {
      pdebug("ConstListContents => ConstListContents ConstValue CommaOrSemicolonOptional");
      (yyval.tconstv) = (yyvsp[-2].tconstv);
      (yyval.tconstv)->add_list((yyvsp[-1].tconstv));
    }
#line 2034 "thrift/thrifty.cc"
    break;

  case 40: /* ConstListContents: %empty  */
#line 597 "thrift/thrifty.yy"
    {
      pdebug("ConstListContents =>");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_list();
    }
#line 2044 "thrift/thrifty.cc"
    break;

  case 41: /* ConstMap: '{' ConstMapContents '}'  */
#line 605 "thrift/thrifty.yy"
    {
      pdebug("ConstMap => { ConstMapContents }");
      (yyval.tconstv) = (yyvsp[-1].tconstv);
    }
#line 2053 "thrift/thrifty.cc"
    break;

  case 42: /* ConstMapContents: ConstMapContents ConstValue ':' ConstValue CommaOrSemicolonOptional  */
#line 612 "thrift/thrifty.yy"
    {
      pdebug("ConstMapContents => ConstMapContents ConstValue CommaOrSemicolonOptional");
      (yyval.tconstv) = (yyvsp[-4].tconstv);
      (yyval.tconstv)->add_map((yyvsp[-3].tconstv), (yyvsp[-1].tconstv));
    }
#line 2063 "thrift/thrifty.cc"
    break;

  case 43: /* ConstMapContents: %empty  */
#line 618 "thrift/thrifty.yy"
    {
      pdebug("ConstMapContents =>");
      (yyval.tconstv) = new t_const_value();
      (yyval.tconstv)->set_map();
    }
#line 2073 "thrift/thrifty.cc"
    break;

  case 44: /* StructHead: tok_struct  */
#line 626 "thrift/thrifty.yy"
    {
      (yyval.iconst) = struct_is_struct;
    }
#line 2081 "thrift/thrifty.cc"
    break;

  case 45: /* StructHead: tok_union  */
#line 630 "thrift/thrifty.yy"
    {
      (yyval.iconst) = struct_is_union;
    }
#line 2089 "thrift/thrifty.cc"
    break;

  case 46: /* Struct: StructHead tok_identifier XsdAll '{' FieldList '}' TypeAnnotations  */
#line 636 "thrift/thrifty.yy"
    {
      pdebug("Struct -> tok_struct tok_identifier { FieldList }");
      validate_simple_identifier( (yyvsp[-5].id));
      (yyvsp[-2].tstruct)->set_xsd_all((yyvsp[-4].tbool));
      (yyvsp[-2].tstruct)->set_union((yyvsp[-6].iconst) == struct_is_union);
      (yyval.tstruct) = (yyvsp[-2].tstruct);
      (yyval.tstruct)->set_name((yyvsp[-5].id));
      if ((yyvsp[0].ttype) != nullptr) {
        (yyval.tstruct)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2106 "thrift/thrifty.cc"
    break;

  case 47: /* XsdAll: tok_xsd_all  */
#line 651 "thrift/thrifty.yy"
    {
      (yyval.tbool) = true;
    }
#line 2114 "thrift/thrifty.cc"
    break;

  case 48: /* XsdAll: %empty  */
#line 655 "thrift/thrifty.yy"
    {
      (yyval.tbool) = false;
    }
#line 2122 "thrift/thrifty.cc"
    break;

  case 49: /* XsdOptional: tok_xsd_optional  */
#line 661 "thrift/thrifty.yy"
    {
      (yyval.tbool) = true;
    }
#line 2130 "thrift/thrifty.cc"
    break;

  case 50: /* XsdOptional: %empty  */
#line 665 "thrift/thrifty.yy"
    {
      (yyval.tbool) = false;
    }
#line 2138 "thrift/thrifty.cc"
    break;

  case 51: /* XsdNillable: tok_xsd_nillable  */
#line 671 "thrift/thrifty.yy"
    {
      (yyval.tbool) = true;
    }
#line 2146 "thrift/thrifty.cc"
    break;

  case 52: /* XsdNillable: %empty  */
#line 675 "thrift/thrifty.yy"
    {
      (yyval.tbool) = false;
    }
#line 2154 "thrift/thrifty.cc"
    break;

  case 53: /* XsdAttributes: tok_xsd_attrs '{' FieldList '}'  */
#line 681 "thrift/thrifty.yy"
    {
      (yyval.tstruct) = (yyvsp[-1].tstruct);
    }
#line 2162 "thrift/thrifty.cc"
    break;

  case 54: /* XsdAttributes: %empty  */
#line 685 "thrift/thrifty.yy"
    {
      (yyval.tstruct) = nullptr;
    }
#line 2170 "thrift/thrifty.cc"
    break;

  case 55: /* Xception: tok_xception tok_identifier '{' FieldList '}' TypeAnnotations  */
#line 691 "thrift/thrifty.yy"
    {
      pdebug("Xception -> tok_xception tok_identifier { FieldList }");
      validate_simple_identifier( (yyvsp[-4].id));
      (yyvsp[-2].tstruct)->set_name((yyvsp[-4].id));
      (yyvsp[-2].tstruct)->set_xception(true);
      (yyval.tstruct) = (yyvsp[-2].tstruct);
      if ((yyvsp[0].ttype) != nullptr) {
        (yyval.tstruct)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2186 "thrift/thrifty.cc"
    break;

  case 56: /* Service: tok_service tok_identifier Extends '{' FlagArgs FunctionList UnflagArgs '}' TypeAnnotations  */
#line 705 "thrift/thrifty.yy"
    {
      pdebug("Service -> tok_service tok_identifier { FunctionList }");
      validate_simple_identifier( (yyvsp[-7].id));
      (yyval.tservice) = (yyvsp[-3].tservice);
      (yyval.tservice)->set_name((yyvsp[-7].id));
      (yyval.tservice)->set_extends((yyvsp[-6].tservice));
      if ((yyvsp[0].ttype) != nullptr) {
        (yyval.tservice)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2202 "thrift/thrifty.cc"
    break;

  case 57: /* FlagArgs: %empty  */
#line 718 "thrift/thrifty.yy"
    {
       g_arglist = 1;
    }
#line 2210 "thrift/thrifty.cc"
    break;

  case 58: /* UnflagArgs: %empty  */
#line 723 "thrift/thrifty.yy"
    {
       g_arglist = 0;
    }
#line 2218 "thrift/thrifty.cc"
    break;

  case 59: /* Extends: tok_extends tok_identifier  */
#line 729 "thrift/thrifty.yy"
    {
      pdebug("Extends -> tok_extends tok_identifier");
      (yyval.tservice) = nullptr;
      if (g_parse_mode == PROGRAM) {
        (yyval.tservice) = g_scope->get_service((yyvsp[0].id));
        if ((yyval.tservice) == nullptr) {
          yyerror("Service \"%s\" has not been defined.", (yyvsp[0].id));
          exit(1);
        }
      }
    }
#line 2234 "thrift/thrifty.cc"
    break;

  case 60: /* Extends: %empty  */
#line 741 "thrift/thrifty.yy"
    {
      (yyval.tservice) = nullptr;
    }
#line 2242 "thrift/thrifty.cc"
    break;

  case 61: /* FunctionList: FunctionList Function  */
#line 747 "thrift/thrifty.yy"
    {
      pdebug("FunctionList -> FunctionList Function");
      (yyval.tservice) = (yyvsp[-1].tservice);
      (yyvsp[-1].tservice)->add_function((yyvsp[0].tfunction));
    }
#line 2252 "thrift/thrifty.cc"
    break;

  case 62: /* FunctionList: %empty  */
#line 753 "thrift/thrifty.yy"
    {
      pdebug("FunctionList -> ");
      (yyval.tservice) = new t_service(g_program);
    }
#line 2261 "thrift/thrifty.cc"
    break;

  case 63: /* Function: CaptureDocText Oneway FunctionType tok_identifier '(' FieldList ')' Throws TypeAnnotations CommaOrSemicolonOptional  */
#line 760 "thrift/thrifty.yy"
    {
      validate_simple_identifier( (yyvsp[-6].id));
      (yyvsp[-4].tstruct)->set_name(std::string((yyvsp[-6].id)) + "_args");
      (yyval.tfunction) = new t_function((yyvsp[-7].ttype), (yyvsp[-6].id), (yyvsp[-4].tstruct), (yyvsp[-2].tstruct), (yyvsp[-8].tbool));
      if ((yyvsp[-9].dtext) != nullptr) {
        (yyval.tfunction)->set_doc((yyvsp[-9].dtext));
      }
      if ((yyvsp[-1].ttype) != nullptr) {
        (yyval.tfunction)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 2278 "thrift/thrifty.cc"
    break;

  case 64: /* Oneway: tok_oneway  */
#line 775 "thrift/thrifty.yy"
    {
      (yyval.tbool) = true;
    }
#line 2286 "thrift/thrifty.cc"
    break;

  case 65: /* Oneway: tok_async  */
#line 779 "thrift/thrifty.yy"
    {
      (yyval.tbool) = true;
    }
#line 2294 "thrift/thrifty.cc"
    break;

  case 66: /* Oneway: %empty  */
#line 783 "thrift/thrifty.yy"
    {
      (yyval.tbool) = false;
    }
#line 2302 "thrift/thrifty.cc"
    break;

  case 67: /* Throws: tok_throws '(' FieldList ')'  */
#line 789 "thrift/thrifty.yy"
    {
      pdebug("Throws -> tok_throws ( FieldList )");
      (yyval.tstruct) = (yyvsp[-1].tstruct);
      if (g_parse_mode == PROGRAM && !validate_throws((yyval.tstruct))) {
        yyerror("Throws clause may not contain non-exception types");
        exit(1);
      }
    }
#line 2315 "thrift/thrifty.cc"
    break;

  case 68: /* Throws: %empty  */
#line 798 "thrift/thrifty.yy"
    {
      (yyval.tstruct) = new t_struct(g_program);
    }
#line 2323 "thrift/thrifty.cc"
    break;

  case 69: /* FieldList: FieldList Field  */
#line 804 "thrift/thrifty.yy"
    {
      pdebug("FieldList -> FieldList , Field");
      (yyval.tstruct) = (yyvsp[-1].tstruct);
      if (!((yyval.tstruct)->append((yyvsp[0].tfield)))) {
        yyerror("\"%d: %s\" - field identifier/name has already been used", (yyvsp[0].tfield)->get_key(), (yyvsp[0].tfield)->get_name().c_str());
        exit(1);
      }
    }
#line 2336 "thrift/thrifty.cc"
    break;

  case 70: /* FieldList: %empty  */
#line 813 "thrift/thrifty.yy"
    {
      pdebug("FieldList -> ");
      y_field_val = -1;
      (yyval.tstruct) = new t_struct(g_program);
    }
#line 2346 "thrift/thrifty.cc"
    break;

  case 71: /* Field: CaptureDocText FieldIdentifier FieldRequiredness FieldType FieldReference FieldName FieldValue XsdOptional XsdNillable XsdAttributes TypeAnnotations CommaOrSemicolonOptional  */
#line 821 "thrift/thrifty.yy"
    {
      pdebug("tok_int_constant : Field -> FieldType FieldName");
      if ((yyvsp[-10].tfieldid).auto_assigned) {
        pwarning(1, "No field key specified for %s, resulting protocol may have conflicts or not be backwards compatible!\n", (yyvsp[-6].id));
        if (g_strict >= 192) {
          yyerror("Implicit field keys are deprecated and not allowed with -strict");
          exit(1);
        }
      }
      validate_simple_identifier((yyvsp[-6].id));
      (yyval.tfield) = new t_field((yyvsp[-8].ttype), (yyvsp[-6].id), (yyvsp[-10].tfieldid).value);
      (yyval.tfield)->set_reference((yyvsp[-7].tbool));
      (yyval.tfield)->set_req((yyvsp[-9].ereq));
      if ((yyvsp[-5].tconstv) != nullptr) {
        g_scope->resolve_const_value((yyvsp[-5].tconstv), (yyvsp[-8].ttype));
        validate_field_value((yyval.tfield), (yyvsp[-5].tconstv));
        (yyval.tfield)->set_value((yyvsp[-5].tconstv));
      }
      (yyval.tfield)->set_xsd_optional((yyvsp[-4].tbool));
      (yyval.tfield)->set_xsd_nillable((yyvsp[-3].tbool));
      if ((yyvsp[-11].dtext) != nullptr) {
        (yyval.tfield)->set_doc((yyvsp[-11].dtext));
      }
      if ((yyvsp[-2].tstruct) != nullptr) {
        (yyval.tfield)->set_xsd_attrs((yyvsp[-2].tstruct));
      }
      if ((yyvsp[-1].ttype) != nullptr) {
        (yyval.tfield)->annotations_ = (yyvsp[-1].ttype)->annotations_;
        delete (yyvsp[-1].ttype);
      }
    }
#line 2382 "thrift/thrifty.cc"
    break;

  case 72: /* FieldName: tok_identifier  */
#line 855 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_identifier");
      (yyval.id) = (yyvsp[0].id);
    }
#line 2391 "thrift/thrifty.cc"
    break;

  case 73: /* FieldName: tok_namespace  */
#line 860 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_namespace");
      (yyval.id) = strdup("namespace");
    }
#line 2400 "thrift/thrifty.cc"
    break;

  case 74: /* FieldName: tok_cpp_include  */
#line 865 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_cpp_include");
      (yyval.id) = strdup("cpp_include");
    }
#line 2409 "thrift/thrifty.cc"
    break;

  case 75: /* FieldName: tok_include  */
#line 877 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_include");
      (yyval.id) = strdup("include");
    }
#line 2418 "thrift/thrifty.cc"
    break;

  case 76: /* FieldName: tok_void  */
#line 882 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_void");
      (yyval.id) = strdup("void");
    }
#line 2427 "thrift/thrifty.cc"
    break;

  case 77: /* FieldName: tok_bool  */
#line 887 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_bool");
      (yyval.id) = strdup("bool");
    }
#line 2436 "thrift/thrifty.cc"
    break;

  case 78: /* FieldName: tok_byte  */
#line 892 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_byte");
      (yyval.id) = strdup("byte");
    }
#line 2445 "thrift/thrifty.cc"
    break;

  case 79: /* FieldName: tok_i8  */
#line 897 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_i8");
      (yyval.id) = strdup("i8");
    }
#line 2454 "thrift/thrifty.cc"
    break;

  case 80: /* FieldName: tok_i16  */
#line 902 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_i16");
      (yyval.id) = strdup("i16");
    }
#line 2463 "thrift/thrifty.cc"
    break;

  case 81: /* FieldName: tok_i32  */
#line 907 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_i32");
      (yyval.id) = strdup("i32");
    }
#line 2472 "thrift/thrifty.cc"
    break;

  case 82: /* FieldName: tok_i64  */
#line 912 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_i64");
      (yyval.id) = strdup("i64");
    }
#line 2481 "thrift/thrifty.cc"
    break;

  case 83: /* FieldName: tok_double  */
#line 917 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_double");
      (yyval.id) = strdup("double");
    }
#line 2490 "thrift/thrifty.cc"
    break;

  case 84: /* FieldName: tok_string  */
#line 922 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_string");
      (yyval.id) = strdup("string");
    }
#line 2499 "thrift/thrifty.cc"
    break;

  case 85: /* FieldName: tok_binary  */
#line 927 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_binary");
      (yyval.id) = strdup("binary");
    }
#line 2508 "thrift/thrifty.cc"
    break;

  case 86: /* FieldName: tok_uuid  */
#line 932 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_uuid");
      (yyval.id) = strdup("uuid");
    }
#line 2517 "thrift/thrifty.cc"
    break;

  case 87: /* FieldName: tok_map  */
#line 937 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_map");
      (yyval.id) = strdup("map");
    }
#line 2526 "thrift/thrifty.cc"
    break;

  case 88: /* FieldName: tok_list  */
#line 942 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_list");
      (yyval.id) = strdup("list");
    }
#line 2535 "thrift/thrifty.cc"
    break;

  case 89: /* FieldName: tok_set  */
#line 947 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_set");
      (yyval.id) = strdup("set");
    }
#line 2544 "thrift/thrifty.cc"
    break;

  case 90: /* FieldName: tok_oneway  */
#line 952 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_oneway");
      (yyval.id) = strdup("oneway");
    }
#line 2553 "thrift/thrifty.cc"
    break;

  case 91: /* FieldName: tok_async  */
#line 957 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_async");
      (yyval.id) = strdup("async");
    }
#line 2562 "thrift/thrifty.cc"
    break;

  case 92: /* FieldName: tok_typedef  */
#line 962 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_typedef");
      (yyval.id) = strdup("typedef");
    }
#line 2571 "thrift/thrifty.cc"
    break;

  case 93: /* FieldName: tok_struct  */
#line 967 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_struct");
      (yyval.id) = strdup("struct");
    }
#line 2580 "thrift/thrifty.cc"
    break;

  case 94: /* FieldName: tok_union  */
#line 972 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_union");
      (yyval.id) = strdup("union");
    }
#line 2589 "thrift/thrifty.cc"
    break;

  case 95: /* FieldName: tok_xception  */
#line 977 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_xception");
      (yyval.id) = strdup("exception");
    }
#line 2598 "thrift/thrifty.cc"
    break;

  case 96: /* FieldName: tok_extends  */
#line 982 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_extends");
      (yyval.id) = strdup("extends");
    }
#line 2607 "thrift/thrifty.cc"
    break;

  case 97: /* FieldName: tok_throws  */
#line 987 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_throws");
      (yyval.id) = strdup("throws");
    }
#line 2616 "thrift/thrifty.cc"
    break;

  case 98: /* FieldName: tok_service  */
#line 992 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_service");
      (yyval.id) = strdup("service");
    }
#line 2625 "thrift/thrifty.cc"
    break;

  case 99: /* FieldName: tok_enum  */
#line 997 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_enum");
      (yyval.id) = strdup("enum");
    }
#line 2634 "thrift/thrifty.cc"
    break;

  case 100: /* FieldName: tok_const  */
#line 1002 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_const");
      (yyval.id) = strdup("const");
    }
#line 2643 "thrift/thrifty.cc"
    break;

  case 101: /* FieldName: tok_required  */
#line 1007 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_required");
      (yyval.id) = strdup("required");
    }
#line 2652 "thrift/thrifty.cc"
    break;

  case 102: /* FieldName: tok_optional  */
#line 1012 "thrift/thrifty.yy"
    {
      pdebug("FieldName -> tok_optional");
      (yyval.id) = strdup("optional");
    }
#line 2661 "thrift/thrifty.cc"
    break;

  case 103: /* FieldIdentifier: tok_int_constant ':'  */
#line 1020 "thrift/thrifty.yy"
    {
      if ((yyvsp[-1].iconst) <= 0) {
        if (g_allow_neg_field_keys) {
          /*
           * g_allow_neg_field_keys exists to allow users to add explicitly
           * specified key values to old .thrift files without breaking
           * protocol compatibility.
           */
          if ((yyvsp[-1].iconst) != y_field_val) {
            /*
             * warn if the user-specified negative value isn't what
             * thrift would have auto-assigned.
             */
            pwarning(1, "Nonpositive field key (%" PRIi64") differs from what would be "
                     "auto-assigned by thrift (%d).\n", (yyvsp[-1].iconst), y_field_val);
          }
          /*
           * Leave $1 as-is, and update y_field_val to be one less than $1.
           * The FieldList parsing will catch any duplicate key values.
           */
          y_field_val = static_cast<int32_t>((yyvsp[-1].iconst) - 1);
          (yyval.tfieldid).value = static_cast<int32_t>((yyvsp[-1].iconst));
          (yyval.tfieldid).auto_assigned = false;
        } else {
          pwarning(1, "Nonpositive value (%d) not allowed as a field key.\n",
                   (yyvsp[-1].iconst));
          (yyval.tfieldid).value = y_field_val--;
          (yyval.tfieldid).auto_assigned = true;
        }
      } else {
        (yyval.tfieldid).value = static_cast<int32_t>((yyvsp[-1].iconst));
        (yyval.tfieldid).auto_assigned = false;
      }
      if( (SHRT_MIN > (yyval.tfieldid).value) || ((yyval.tfieldid).value > SHRT_MAX)) {
        pwarning(1, "Field key (%d) exceeds allowed range (%d..%d).\n",
                 (yyval.tfieldid).value, SHRT_MIN, SHRT_MAX);
      }
    }
#line 2704 "thrift/thrifty.cc"
    break;

  case 104: /* FieldIdentifier: %empty  */
#line 1059 "thrift/thrifty.yy"
    {
      (yyval.tfieldid).value = y_field_val--;
      (yyval.tfieldid).auto_assigned = true;
      if( (SHRT_MIN > (yyval.tfieldid).value) || ((yyval.tfieldid).value > SHRT_MAX)) {
        pwarning(1, "Field key (%d) exceeds allowed range (%d..%d).\n",
                 (yyval.tfieldid).value, SHRT_MIN, SHRT_MAX);
      }
    }
#line 2717 "thrift/thrifty.cc"
    break;

  case 105: /* FieldReference: tok_reference  */
#line 1070 "thrift/thrifty.yy"
    {
      (yyval.tbool) = true;
    }
#line 2725 "thrift/thrifty.cc"
    break;

  case 106: /* FieldReference: %empty  */
#line 1074 "thrift/thrifty.yy"
   {
     (yyval.tbool) = false;
   }
#line 2733 "thrift/thrifty.cc"
    break;

  case 107: /* FieldRequiredness: tok_required  */
#line 1080 "thrift/thrifty.yy"
    {
      (yyval.ereq) = t_field::T_REQUIRED;
    }
#line 2741 "thrift/thrifty.cc"
    break;

  case 108: /* FieldRequiredness: tok_optional  */
#line 1084 "thrift/thrifty.yy"
    {
      if (g_arglist) {
        if (g_parse_mode == PROGRAM) {
          pwarning(1, "optional keyword is ignored in argument lists.\n");
        }
        (yyval.ereq) = t_field::T_OPT_IN_REQ_OUT;
      } else {
        (yyval.ereq) = t_field::T_OPTIONAL;
      }
    }
#line 2756 "thrift/thrifty.cc"
    break;

  case 109: /* FieldRequiredness: %empty  */
#line 1095 "thrift/thrifty.yy"
    {
      (yyval.ereq) = t_field::T_OPT_IN_REQ_OUT;
    }
#line 2764 "thrift/thrifty.cc"
    break;

  case 110: /* FieldValue: '=' ConstValue  */
#line 1101 "thrift/thrifty.yy"
    {
      if (g_parse_mode == PROGRAM) {
        (yyval.tconstv) = (yyvsp[0].tconstv);
      } else {
        (yyval.tconstv) = nullptr;
      }
    }
#line 2776 "thrift/thrifty.cc"
    break;

  case 111: /* FieldValue: %empty  */
#line 1109 "thrift/thrifty.yy"
    {
      (yyval.tconstv) = nullptr;
    }
#line 2784 "thrift/thrifty.cc"
    break;

  case 112: /* FunctionType: FieldType  */
#line 1115 "thrift/thrifty.yy"
    {
      pdebug("FunctionType -> FieldType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2793 "thrift/thrifty.cc"
    break;

  case 113: /* FunctionType: tok_void  */
#line 1120 "thrift/thrifty.yy"
    {
      pdebug("FunctionType -> tok_void");
      (yyval.ttype) = g_type_void;
    }
#line 2802 "thrift/thrifty.cc"
    break;

  case 114: /* FieldType: tok_identifier  */
#line 1127 "thrift/thrifty.yy"
    {
      pdebug("FieldType -> tok_identifier");
      if (g_parse_mode == INCLUDES) {
        // Ignore identifiers in include mode
        (yyval.ttype) = nullptr;
      } else {
        // Lookup the identifier in the current scope
        (yyval.ttype) = g_scope->get_type((yyvsp[0].id));
        if ((yyval.ttype) == nullptr) {
          /*
           * Either this type isn't yet declared, or it's never
             declared.  Either way allow it and we'll figure it out
             during generation.
           */
          (yyval.ttype) = new t_typedef(g_program, (yyvsp[0].id), true);
        }
      }
    }
#line 2825 "thrift/thrifty.cc"
    break;

  case 115: /* FieldType: BaseType  */
#line 1146 "thrift/thrifty.yy"
    {
      pdebug("FieldType -> BaseType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2834 "thrift/thrifty.cc"
    break;

  case 116: /* FieldType: ContainerType  */
#line 1151 "thrift/thrifty.yy"
    {
      pdebug("FieldType -> ContainerType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2843 "thrift/thrifty.cc"
    break;

  case 117: /* BaseType: SimpleBaseType TypeAnnotations  */
#line 1157 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> SimpleBaseType TypeAnnotations");
      if ((yyvsp[0].ttype) != nullptr) {
        (yyval.ttype) = new t_base_type(*static_cast<t_base_type*>((yyvsp[-1].ttype)));
        (yyval.ttype)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      } else {
        (yyval.ttype) = (yyvsp[-1].ttype);
      }
    }
#line 2858 "thrift/thrifty.cc"
    break;

  case 118: /* SimpleBaseType: tok_string  */
#line 1170 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_string");
      (yyval.ttype) = g_type_string;
    }
#line 2867 "thrift/thrifty.cc"
    break;

  case 119: /* SimpleBaseType: tok_binary  */
#line 1175 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_binary");
      (yyval.ttype) = g_type_binary;
    }
#line 2876 "thrift/thrifty.cc"
    break;

  case 120: /* SimpleBaseType: tok_uuid  */
#line 1180 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_uuid");
      (yyval.ttype) = g_type_uuid;
    }
#line 2885 "thrift/thrifty.cc"
    break;

  case 121: /* SimpleBaseType: tok_bool  */
#line 1185 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_bool");
      (yyval.ttype) = g_type_bool;
    }
#line 2894 "thrift/thrifty.cc"
    break;

  case 122: /* SimpleBaseType: tok_byte  */
#line 1190 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_byte");
      (yyval.ttype) = g_type_i8;  // byte is signed in Thrift, just an alias for i8
    }
#line 2903 "thrift/thrifty.cc"
    break;

  case 123: /* SimpleBaseType: tok_i8  */
#line 1195 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_i8");
      (yyval.ttype) = g_type_i8;
    }
#line 2912 "thrift/thrifty.cc"
    break;

  case 124: /* SimpleBaseType: tok_i16  */
#line 1200 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_i16");
      (yyval.ttype) = g_type_i16;
    }
#line 2921 "thrift/thrifty.cc"
    break;

  case 125: /* SimpleBaseType: tok_i32  */
#line 1205 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_i32");
      (yyval.ttype) = g_type_i32;
    }
#line 2930 "thrift/thrifty.cc"
    break;

  case 126: /* SimpleBaseType: tok_i64  */
#line 1210 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_i64");
      (yyval.ttype) = g_type_i64;
    }
#line 2939 "thrift/thrifty.cc"
    break;

  case 127: /* SimpleBaseType: tok_double  */
#line 1215 "thrift/thrifty.yy"
    {
      pdebug("BaseType -> tok_double");
      (yyval.ttype) = g_type_double;
    }
#line 2948 "thrift/thrifty.cc"
    break;

  case 128: /* ContainerType: SimpleContainerType TypeAnnotations  */
#line 1221 "thrift/thrifty.yy"
    {
      pdebug("ContainerType -> SimpleContainerType TypeAnnotations");
      (yyval.ttype) = (yyvsp[-1].ttype);
      if ((yyvsp[0].ttype) != nullptr) {
        (yyval.ttype)->annotations_ = (yyvsp[0].ttype)->annotations_;
        delete (yyvsp[0].ttype);
      }
    }
#line 2961 "thrift/thrifty.cc"
    break;

  case 129: /* SimpleContainerType: MapType  */
#line 1232 "thrift/thrifty.yy"
    {
      pdebug("SimpleContainerType -> MapType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2970 "thrift/thrifty.cc"
    break;

  case 130: /* SimpleContainerType: SetType  */
#line 1237 "thrift/thrifty.yy"
    {
      pdebug("SimpleContainerType -> SetType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2979 "thrift/thrifty.cc"
    break;

  case 131: /* SimpleContainerType: ListType  */
#line 1242 "thrift/thrifty.yy"
    {
      pdebug("SimpleContainerType -> ListType");
      (yyval.ttype) = (yyvsp[0].ttype);
    }
#line 2988 "thrift/thrifty.cc"
    break;

  case 132: /* MapType: tok_map CppType '<' FieldType ',' FieldType '>'  */
#line 1249 "thrift/thrifty.yy"
    {
      pdebug("MapType -> tok_map <FieldType, FieldType>");
      (yyval.ttype) = new t_map((yyvsp[-3].ttype), (yyvsp[-1].ttype));
      if ((yyvsp[-5].id) != nullptr) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[-5].id)));
      }
    }
#line 3000 "thrift/thrifty.cc"
    break;

  case 133: /* SetType: tok_set CppType '<' FieldType '>'  */
#line 1259 "thrift/thrifty.yy"
    {
      pdebug("SetType -> tok_set<FieldType>");
      (yyval.ttype) = new t_set((yyvsp[-1].ttype));
      if ((yyvsp[-3].id) != nullptr) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[-3].id)));
      }
    }
#line 3012 "thrift/thrifty.cc"
    break;

  case 134: /* ListType: tok_list CppType '<' FieldType '>' CppType  */
#line 1269 "thrift/thrifty.yy"
    {
      pdebug("ListType -> tok_list<FieldType>");
      check_for_list_of_bytes((yyvsp[-2].ttype));
      (yyval.ttype) = new t_list((yyvsp[-2].ttype));
      if ((yyvsp[-4].id) != nullptr) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[-4].id)));
      }
      if ((yyvsp[0].id) != nullptr) {
        ((t_container*)(yyval.ttype))->set_cpp_name(std::string((yyvsp[0].id)));
        pwarning(1, "The syntax 'list<type> cpp_type \"c++ type\"' is deprecated. Use 'list cpp_type \"c++ type\" <type>' instead.\n");
      }
      if (((yyvsp[-4].id) != nullptr) && ((yyvsp[0].id) != nullptr)) {
        pwarning(1, "Two cpp_types clauses at list<%>\n", (yyvsp[-4].id));
      }
    }
#line 3032 "thrift/thrifty.cc"
    break;

  case 135: /* CppType: tok_cpp_type tok_literal  */
#line 1287 "thrift/thrifty.yy"
    {
      (yyval.id) = (yyvsp[0].id);
    }
#line 3040 "thrift/thrifty.cc"
    break;

  case 136: /* CppType: %empty  */
#line 1291 "thrift/thrifty.yy"
    {
      (yyval.id) = nullptr;
    }
#line 3048 "thrift/thrifty.cc"
    break;

  case 137: /* TypeAnnotations: '(' TypeAnnotationList ')'  */
#line 1297 "thrift/thrifty.yy"
    {
      pdebug("TypeAnnotations -> ( TypeAnnotationList )");
      (yyval.ttype) = (yyvsp[-1].ttype);
    }
#line 3057 "thrift/thrifty.cc"
    break;

  case 138: /* TypeAnnotations: %empty  */
#line 1302 "thrift/thrifty.yy"
    {
      (yyval.ttype) = nullptr;
    }
#line 3065 "thrift/thrifty.cc"
    break;

  case 139: /* TypeAnnotationList: TypeAnnotationList TypeAnnotation  */
#line 1308 "thrift/thrifty.yy"
    {
      pdebug("TypeAnnotationList -> TypeAnnotationList , TypeAnnotation");
      (yyval.ttype) = (yyvsp[-1].ttype);
      (yyval.ttype)->annotations_[(yyvsp[0].tannot)->key].push_back((yyvsp[0].tannot)->val);
      delete (yyvsp[0].tannot);
    }
#line 3076 "thrift/thrifty.cc"
    break;

  case 140: /* TypeAnnotationList: %empty  */
#line 1315 "thrift/thrifty.yy"
    {
      /* Just use a dummy structure to hold the annotations. */
      (yyval.ttype) = new t_struct(g_program);
    }
#line 3085 "thrift/thrifty.cc"
    break;

  case 141: /* TypeAnnotation: tok_identifier TypeAnnotationValue CommaOrSemicolonOptional  */
#line 1322 "thrift/thrifty.yy"
    {
      pdebug("TypeAnnotation -> TypeAnnotationValue");
      (yyval.tannot) = new t_annotation;
      (yyval.tannot)->key = (yyvsp[-2].id);
      (yyval.tannot)->val = (yyvsp[-1].id);
    }
#line 3096 "thrift/thrifty.cc"
    break;

  case 142: /* TypeAnnotationValue: '=' tok_literal  */
#line 1331 "thrift/thrifty.yy"
    {
      pdebug("TypeAnnotationValue -> = tok_literal");
      (yyval.id) = (yyvsp[0].id);
    }
#line 3105 "thrift/thrifty.cc"
    break;

  case 143: /* TypeAnnotationValue: %empty  */
#line 1336 "thrift/thrifty.yy"
    {
      pdebug("TypeAnnotationValue ->");
      (yyval.id) = strdup("1");
    }
#line 3114 "thrift/thrifty.cc"
    break;


#line 3118 "thrift/thrifty.cc"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (YY_("syntax error"));
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 1341 "thrift/thrifty.yy"

