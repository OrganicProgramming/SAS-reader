# SAS reader
## General overview
This code is a straightforward port of 
https://github.com/kshedden/datareader
made as an exercise to observe the 
differences between Go and Rust for
file/byte manipulations after reading 
the Rust book. This library
reads the SAS7bdat linewise such that
files of arbitrary sizes can be processed.
There is support for multiple encodings. 
Corrections to the RLE decompression where added with the 
help of https://github.com/WizardMac/ReadStat.

Some minimal example code, covering most
of the public API:

```rust
use std::fs::File;
use std::io::BufReader;
use sas::*;

fn main() -> Result<(), SasError> {
    let sas_reader = BufReader::new(File::open("/path/to/sas7bdat file").unwrap());
    let mut sas = SAS7bdat::new(sas_reader)?;
    while sas.read_line()?{
        for (idx, el) in sas.col_names.iter().enumerate(){
            println!("{el}:");
            match &sas.row_vals[idx]{
                SasVal::Numeric(x) => println!("{x}"),
                SasVal::Text(x) => println!("{x}"),
                _ => println!("Probably a date value"),
            };
        }
    }
    Ok(())
}
```

## TODO:
- Refactor constructor and iterator.
- Testing  
- Use of serde like rust-csv?
