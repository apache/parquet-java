Parquet Tools
======

Parquet-Tools contains java based command line tools that aid
in the inspection of [Parquet files](https://github.com/Parquet).

Currently these tools are available for UN*X systems.

## Usage

```sh
usage: parquet-tools cat [option...] <input>
where option is one of:
       --debug     Disable color output even if supported
    -h,--help      Show this help string
       --no-color  Disable color output even if supported
where <input> is the parquet file to print to stdout

usage: parquet-tools head [option...] <input>
where option is one of:
       --debug          Disable color output even if supported
    -h,--help           Show this help string
    -n,--records <arg>  The number of records to show (default: 5)
       --no-color       Disable color output even if supported
where <input> is the parquet file to print to stdout

usage: parquet-tools schema [option...] <input>
where option is one of:
    -d,--detailed <arg>  Show detailed information about the schema.
       --debug           Disable color output even if supported
    -h,--help            Show this help string
       --no-color        Disable color output even if supported
where <input> is the parquet file containing the schema to show

usage: parquet-tools meta [option...] <input>
where option is one of:
       --debug     Disable color output even if supported
    -h,--help      Show this help string
       --no-color  Disable color output even if supported
where <input> is the parquet file to print to stdout

usage: parquet-tools dump [option...] <input>
where option is one of:
    -c,--column <arg>  Dump only the given column, can be specified more than
                       once
    -d,--disable-data  Do not dump column data
       --debug         Disable color output even if supported
    -h,--help          Show this help string
    -m,--disable-meta  Do not dump row group and page metadata
       --no-color      Disable color output even if supported
where <input> is the parquet file to print to stdout
```

