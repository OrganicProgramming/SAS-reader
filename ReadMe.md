This code is a straightforward port of 
https://github.com/kshedden/datareader
to experience the similarities and 
differences between Go and Rust for
file/byte manipulations. This library
reads the SAS7bdat linewise such that
files of arbitrary sizes can be processed.

Some minimal example code, covering most
of the public API:

```
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
                SasVal::Numeric(x) => println!("{x}"),//println!("{el} = {}", x),
                SasVal::Text(x) => println!("{x}"),//println!("{el} = {}", x),
                _ => println!("Probably a date value"),
            };
        }
    }
    Ok(())
}
```
