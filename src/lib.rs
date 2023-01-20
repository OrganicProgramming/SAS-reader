use std::collections::HashMap;
use std::fs::File;
use std::io::{SeekFrom, Read, Seek, BufReader};
use encoding::{Encoding, DecoderTrap};
use encoding::all::*;
use encoding::codec::singlebyte::SingleByteEncoding;
use encoding::codec::utf_8::UTF8Encoding;
use chrono::prelude::*;
use chrono::{Duration, Utc, NaiveDate};

#[derive(Default, Debug)]
enum Endian{
    #[default] 
    Little,
    Big,
}

#[derive(Clone, Debug)]
pub enum SasVal{
    Numeric(f64),
    Text(String),
    Date(NaiveDate),
    DateTime(DateTime<Utc>),
}

type Decompressor = fn(usize, &[u8]) -> Result<Vec<u8>, SasError>;

//#[derive(Default)]
pub struct SAS7bdat<R>{
    date_base : DateTime<Utc>,
    pub row_vals : Vec<SasVal>,
    col_formats : Vec<String>,
    trim_strings : bool,
    no_align_correction : bool,
    date_created : f64,
    date_modified : f64,
    name : String,
    platform : String,
    sas_release : String,
    server_type : String,
    os_type : String,
    os_name : String,
    file_type : String,
    file_encoding : String,
    u64 : bool,
    byte_order : Endian,
    compression : String,
    text_decoder : Encodings,
    row_count : usize,
    col_types : Vec<u16>,
    col_labels : Vec<String>,
    pub col_names : Vec<String>,
    buf : Vec<u8>,
    float_buf : [u8;8],
    buf_rdr : BufReader<R>,
    cached_page : Vec<u8>,
    cur_page_type : isize,
    cur_page_block_count : usize,
    cur_page_sub_hdr_count : usize,
    cur_row_in_file_idx : usize,
    cur_row_on_page_idx : usize,
    cur_page_data_sub_hdr_pointers : Vec<SubHdrPtr>,
    cur_row_in_chunk_idx : usize,
    col_name_strings : Vec<Vec<u8>>,
    col_data_off : Vec<usize>,
    col_data_lens : Vec<usize>,
    cols : Vec<Col>,
    props : SasProperties,
    encoding_map : HashMap<usize, &'static str>,
    hdr_sig_map : HashMap<Vec<u8>, usize>, 
}

#[derive(Default)]
struct SasProperties{
    int_len : usize,
    page_bit_off : usize,
    sub_hdr_ptr_len : usize,
    hdr_len : usize,
    page_len : usize,
    page_count : usize,
    row_len : usize,
    col_count_p1 : usize,
    col_count_p2 : usize,
    mix_page_row_cnt : usize,
    lcs : usize,
    lcp : usize,
    creator_proc : String,
    col_cnt : usize,
}

#[derive(Default)]
struct Col {
    col_id : usize,
    name : String,
    label : String,
    fmt : String,
    ctype : u16,
    len : usize,
}

#[derive(Default)]
struct SubHdrPtr{
    off : usize,
    len : usize,
    compression : usize,
    ptype : usize,
}

const ROW_SIZE_IDX : usize = 0;
const COL_SIZE_IDX : usize = 1;
const SUB_HDR_CNT_IDX : usize = 2;
const COL_TEXT_IDX : usize = 3;
const COL_NAME_IDX : usize = 4;
const COL_ATTR_IDX : usize = 5;
const FMT_AND_LABEL_IDX : usize = 6;
const COL_LIST_IDX : usize = 7;
const DATA_SUBHDR_IDX : usize = 8;
const SAS_NUM_TYPE : u16 = 0;
const SAS_STRING_TYPE : u16 = 1;
fn get_hdr_sig_map() -> HashMap<Vec<u8>, usize> {
    HashMap::from([(b"\x00\x00\x00\x00\xF7\xF7\xF7\xF7".to_vec(), ROW_SIZE_IDX),
    (b"\xF7\xF7\xF7\xF7".to_vec(),                 ROW_SIZE_IDX),
    (b"\xF7\xF7\xF7\xF7\x00\x00\x00\x00".to_vec(), ROW_SIZE_IDX),
    (b"\xF7\xF7\xF7\xF7\xFF\xFF\xFB\xFE".to_vec(), ROW_SIZE_IDX),
    (b"\xF6\xF6\xF6\xF6".to_vec(),                 COL_SIZE_IDX),
    (b"\x00\x00\x00\x00\xF6\xF6\xF6\xF6".to_vec(), COL_SIZE_IDX),
    (b"\xF6\xF6\xF6\xF6\x00\x00\x00\x00".to_vec(), COL_SIZE_IDX),
    (b"\xF6\xF6\xF6\xF6\xFF\xFF\xFB\xFE".to_vec(), COL_SIZE_IDX),
    (b"\x00\xFC\xFF\xFF".to_vec(),                 SUB_HDR_CNT_IDX),
    (b"\xFF\xFF\xFC\x00".to_vec(),                 SUB_HDR_CNT_IDX),
    (b"\x00\xFC\xFF\xFF\xFF\xFF\xFF\xFF".to_vec(), SUB_HDR_CNT_IDX),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFC\x00".to_vec(), SUB_HDR_CNT_IDX),
    (b"\xFD\xFF\xFF\xFF".to_vec(),                 COL_TEXT_IDX),
    (b"\xFF\xFF\xFF\xFD".to_vec(),                 COL_TEXT_IDX),
    (b"\xFD\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec(), COL_TEXT_IDX),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFD".to_vec(), COL_TEXT_IDX),
    (b"\xFF\xFF\xFF\xFF".to_vec(),                 COL_NAME_IDX),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec(), COL_NAME_IDX),
    (b"\xFC\xFF\xFF\xFF".to_vec(),                 COL_ATTR_IDX),
    (b"\xFF\xFF\xFF\xFC".to_vec(),                 COL_ATTR_IDX),
    (b"\xFC\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec(), COL_ATTR_IDX),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFC".to_vec(), COL_ATTR_IDX),
    (b"\xFE\xFB\xFF\xFF".to_vec(),                 FMT_AND_LABEL_IDX),
    (b"\xFF\xFF\xFB\xFE".to_vec(),                 FMT_AND_LABEL_IDX),
    (b"\xFE\xFB\xFF\xFF\xFF\xFF\xFF\xFF".to_vec(), FMT_AND_LABEL_IDX),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFB\xFE".to_vec(), FMT_AND_LABEL_IDX),
    (b"\xFE\xFF\xFF\xFF".to_vec(),                 COL_LIST_IDX),
    (b"\xFF\xFF\xFF\xFE".to_vec(),                 COL_LIST_IDX),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec(), COL_LIST_IDX),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE".to_vec(), COL_LIST_IDX)])
}

const MAGIC : &[u8;32] = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc2\xea\x81\x60\xb3\x14\x11\xcf\xbd\x92\x08\x00\x09\xc7\x31\x8c\x18\x1f\x10\x11";
const ALIGN_1_CHECKER_VALUE : [u8;1] = [51];
const ALIGN_1_OFFSET : usize = 32;
const ALIGN_1_LENGTH : usize = 1;
const U64_BYTE_CHECKER_VALUE : u8 = 51;
const ALIGN_2_OFFSET : usize = 35;
const ALIGN_2_LENGTH : usize = 1;
const ALIGN_2_VALUE : usize = 4;
const ENDIANNESS_OFFSET : usize = 37;
const ENDIANNESS_LENGTH : usize = 1;
const PLATFORM_OFFSET : usize = 39;
const PLATFORM_LENGTH : usize = 1;
const ENCODING_OFFSET : usize = 70;
const ENCODING_LENGTH : usize = 1;
const DATASET_OFFSET : usize = 92;
const DATASET_LENGTH : usize = 64;
const FILE_TYPE_OFFSET : usize = 156;
const FILE_TYPE_LENGTH : usize = 8;
const DATE_CREATED_OFFSET : usize = 164;
const DATE_CREATED_LENGTH : usize = 8;
const DATE_MODIFIED_OFFSET : usize = 172;
const DATE_MODIFIED_LENGTH : usize = 8;
const HEADER_SIZE_OFFSET : usize = 196;
const HEADER_SIZE_LENGTH : usize = 4;
const PAGE_SIZE_OFFSET : usize = 200;
const PAGE_SIZE_LENGTH : usize = 4;
const PAGE_COUNT_OFFSET : usize = 204;
const PAGE_COUNT_LENGTH : usize = 4;
const SAS_RELEASE_OFFSET : usize = 216;
const SAS_RELEASE_LENGTH : usize = 8;
const SAS_SERVER_TYPE_OFFSET : usize = 224;
const SAS_SERVER_TYPE_LENGTH : usize = 16;
const OS_VERSION_NUMBER_OFFSET : usize = 240;
const OS_VERSION_NUMBER_LENGTH : usize = 16;
const OS_MAKER_OFFSET : usize = 256;
const OS_MAKER_LENGTH : usize = 16;
const OS_NAME_OFFSET : usize = 272;
const OS_NAME_LENGTH : usize = 16;
const PAGE_BIT_OFFSET_X86 : usize = 16;
const PAGE_BIT_OFFSET_X64 : usize = 32;
const SUBHEADER_POINTER_LENGTH_X86 : usize = 12;
const SUBHEADER_POINTER_LENGTH_X64 : usize = 24;
const PAGE_TYPE_OFFSET : usize = 0;
const PAGE_TYPE_LENGTH : usize = 2;
const BLOCK_COUNT_OFFSET : usize = 2;
const BLOCK_COUNT_LENGTH : usize = 2;
const SUBHEADER_COUNT_OFFSET : usize = 4;
const SUBHEADER_COUNT_LENGTH : usize = 2;
const PAGE_META_TYPE : isize = 0;
const PAGE_DATA_TYPE : isize = 256;
const PAGE_AMD_TYPE : isize = 1024;
const SUBHEADER_POINTERS_OFFSET : usize = 8;
const TRUNCATED_SUBHEADER_ID : usize = 1;
const COMPRESSED_SUBHEADER_ID : usize = 4;
const COMPRESSED_SUBHEADER_TYPE : usize = 1;
const TEXT_BLOCK_SIZE_LENGTH : usize = 2;
const ROW_LENGTH_OFFSET_MULTIPLIER : usize = 5;
const ROW_COUNT_OFFSET_MULTIPLIER : usize = 6;
const COL_COUNT_P1_MULTIPLIER : usize = 9;
const COL_COUNT_P2_MULTIPLIER : usize = 10;
const ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER : usize = 15;
const COLUMN_NAME_POINTER_LENGTH : usize = 8;
const COLUMN_NAME_TEXT_SUBHEADER_OFFSET : usize = 0;
const COLUMN_NAME_TEXT_SUBHEADER_LENGTH : usize = 2;
const COLUMN_NAME_OFFSET_OFFSET : usize = 2;
const COLUMN_NAME_OFFSET_LENGTH : usize = 2;
const COLUMN_NAME_LENGTH_OFFSET : usize = 4;
const COLUMN_NAME_LENGTH_LENGTH : usize = 2;
const COLUMN_DATA_OFFSET_OFFSET : usize = 8;
const COLUMN_DATA_LENGTH_OFFSET : usize = 8;
const COLUMN_DATA_LENGTH_LENGTH : usize = 4;
const COLUMN_TYPE_OFFSET : usize = 14;
const COLUMN_TYPE_LENGTH : usize = 1;
const COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET : usize = 22;
const COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_LENGTH : usize = 2;
const COLUMN_FORMAT_OFFSET_OFFSET : usize = 24;
const COLUMN_FORMAT_OFFSET_LENGTH : usize = 2;
const COLUMN_FORMAT_LENGTH_OFFSET : usize = 26;
const COLUMN_FORMAT_LENGTH_LENGTH : usize = 2;
const COLUMN_LABEL_TEXT_SUBHEADER_INDEX_OFFSET : usize = 28;
const COLUMN_LABEL_TEXT_SUBHEADER_INDEX_LENGTH : usize = 2;
const COLUMN_LABEL_OFFSET_OFFSET : usize = 30;
const COLUMN_LABEL_OFFSET_LENGTH : usize = 2;
const COLUMN_LABEL_LENGTH_OFFSET : usize = 32;
const COLUMN_LABEL_LENGTH_LENGTH : usize = 2;

trait ByteNum {
    fn from_bytes(bytes : &[u8], off : usize, w : usize, endi : &Endian) -> Self;
}

impl ByteNum for i8{
    fn from_bytes(bytes : &[u8], off : usize, w : usize, endi : &Endian) -> i8{
        match endi {
            Endian::Big => i8::from_be_bytes(bytes[off..(off + w)].try_into().unwrap()),
            Endian::Little => i8::from_le_bytes(bytes[off..(off + w)].try_into().unwrap()),
        }
    }
}

impl ByteNum for i16{
    fn from_bytes(bytes : &[u8], off : usize, w : usize, endi : &Endian) -> i16 {
        match endi {
            Endian::Big => i16::from_be_bytes(bytes[off..(off + w)].try_into().unwrap()),
            Endian::Little => i16::from_le_bytes(bytes[off..(off + w)].try_into().unwrap()),
        }
    }
}

impl ByteNum for i32{
    fn from_bytes(bytes : &[u8], off : usize, w : usize, endi : &Endian) -> i32 {
        match endi {
            Endian::Big => i32::from_be_bytes(bytes[off..(off + w)].try_into().unwrap()),
            Endian::Little => i32::from_le_bytes(bytes[off..(off + w)].try_into().unwrap()),
        }
    }
}

impl ByteNum for i64{
    fn from_bytes(bytes : &[u8], off : usize, w : usize, endi : &Endian) -> i64 {
        match endi {
            Endian::Big => i64::from_be_bytes(bytes[off..(off + w)].try_into().unwrap()),
            Endian::Little => i64::from_le_bytes(bytes[off..(off + w)].try_into().unwrap()),
        }
    }
}

impl ByteNum for f64{
    fn from_bytes(bytes : &[u8], off : usize, w : usize, endi : &Endian) -> f64 {
        match endi {
            Endian::Big => f64::from_be_bytes(bytes[off..(off + w)].try_into().unwrap()),
            Endian::Little => f64::from_le_bytes(bytes[off..(off + w)].try_into().unwrap()),
        }
    }
}

#[derive(Debug)]
pub enum SasError{
    TypeConversion,
    UnknownDecoder,
    UnexpectedEndOfControlByte,
    ControlByte,
    ByteConversion,
    BufLen,
    Read,
    Cmd,
    SasProperty(String),
}

fn min(x : usize, y : usize) -> usize {
    if x < y {
        x
    } else {
        y
    }
}

fn contains_bytes(bytes : &Vec<u8>, txt : &str) -> bool{
    let txt_bytes = txt.as_bytes();
    let txt_len = txt_bytes.len();
    for i in 0..bytes.len()- txt_bytes.len(){
        if &bytes[i..i + txt_len] == txt_bytes{
            return true;
        }
    }
    false
}

enum Encodings{
    SingleByte(SingleByteEncoding),
    MultiByte(UTF8Encoding),
}

impl Encodings{
    fn decode(&self, bytes : &[u8]) -> Result<String, SasError>{
        match self{
            Encodings::SingleByte(x) => {
                match x.decode(bytes, DecoderTrap::Strict){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::ByteConversion),
                }
            }
            Encodings::MultiByte(x) =>  {
                match x.decode(bytes, DecoderTrap::Strict){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::ByteConversion),
                }
            }
        }
    }
}

impl<R: std::io::Read + std::io::Seek> Iterator for SAS7bdat<R>{
    type Item = Vec<SasVal>;

    fn next(&mut self) -> Option<Self::Item>{
        match self.read_line(){
            Ok(true) => {
                Some(self.row_vals.clone())
            }
            Ok(false) => {
                None
            }
            Err(_) => panic!("Could not read all lines!"),
        }
    }
}

impl<R : std::io::Read + std::io::Seek> SAS7bdat<R>{

    fn utf_8(&self, bytes : &[u8]) -> Result<String, SasError>{
        self.text_decoder.decode(bytes)
    }

    fn get_decompressor(&self)  -> Option<Decompressor>{
        match self.compression.as_str() {
            "SASYZCRL" => Some(rle_decompress),
            "SASYZCR2" => Some(rdc_decompress),
            _ => None,
        }
    }

    fn get_properties(&mut self) -> Result<(), SasError>{
        self.props = SasProperties::default();
        self.read_bytes(0,288)?;
        self.cached_page = vec![0;288];        
        self.cached_page.copy_from_slice(&self.buf[0..288]);
        if &self.cached_page[0..MAGIC.len()] != MAGIC{
            return Err(SasError::SasProperty("Magic Byte incorrect, SAS file?".to_string()));
        }
        let mut align1 : usize = 0;
        let mut align2 : usize = 0;
        self.read_bytes(ALIGN_1_OFFSET, ALIGN_1_LENGTH)?;
        self.props.page_bit_off = PAGE_BIT_OFFSET_X86;
        self.props.sub_hdr_ptr_len = SUBHEADER_POINTER_LENGTH_X86;
        self.props.int_len = 4;
        if self.buf[0] == U64_BYTE_CHECKER_VALUE {
            align2 = ALIGN_2_VALUE;
            self.u64 = true;
            self.props.int_len = 8;
            self.props.page_bit_off = PAGE_BIT_OFFSET_X64;
            self.props.sub_hdr_ptr_len = SUBHEADER_POINTER_LENGTH_X64;
        }
        self.read_bytes(ALIGN_2_OFFSET, ALIGN_2_LENGTH)?;
        if  self.buf[0..ALIGN_2_LENGTH] == ALIGN_1_CHECKER_VALUE{
            align1 = ALIGN_2_VALUE;
        }
        let total_align = align1 + align2; 
        self.read_bytes(ENDIANNESS_OFFSET, ENDIANNESS_LENGTH)?;
        if self.buf[0] == b'\x01' {
            self.byte_order = Endian::Little;
        } else {
            self.byte_order = Endian::Big;
        }
        self.read_bytes(PLATFORM_OFFSET, PLATFORM_LENGTH)?;
        match char::from(self.buf[0]) {
            '1' => self.platform = "unix".to_string(),
            '2' => self.platform = "windows".to_string(),
            _ => self.platform = "unknown".to_string(),
        }
        self.read_bytes(ENCODING_OFFSET, ENCODING_LENGTH)?;
        match self.encoding_map.get(&usize::from(self.buf[0])) {
            Some(x) => {
                self.file_encoding = x.to_string();
                match *x {
                    "wlatin1" => self.text_decoder = Encodings::SingleByte(*WINDOWS_1252),
                    "wlatin2" => self.text_decoder = Encodings::SingleByte(*WINDOWS_1250),
                    "utf-8" => self.text_decoder = Encodings::MultiByte(UTF8Encoding),
                    _ => return Err(SasError::UnknownDecoder),
                }
            }
            None => self.file_encoding = format!("Unspecified encoding: {}", self.buf[0]), 

        }
        self.read_bytes(DATASET_OFFSET, DATASET_LENGTH)?;
        self.name = self.utf_8(&self.buf[0..DATASET_LENGTH])?;

        self.read_bytes(FILE_TYPE_OFFSET, FILE_TYPE_LENGTH)?;
        self.file_type = self.utf_8(&self.buf[0..FILE_TYPE_LENGTH])?;
        self.date_created = self.read_float(DATE_CREATED_OFFSET, DATE_CREATED_LENGTH);
        self.date_modified = self.read_float(DATE_MODIFIED_OFFSET, DATE_MODIFIED_LENGTH);
        self.props.hdr_len = self.read_int(HEADER_SIZE_OFFSET + align1, HEADER_SIZE_LENGTH)?;

        if self.u64 && self.props.hdr_len != 8192 {
            return Err(SasError::SasProperty("Inappropriate headerlength for 64bit architecture".to_string()));
        }

        let mut tmp_buf : Vec<u8> = vec![0;self.props.hdr_len - 288]; 
        if self.buf_rdr.read_exact(&mut tmp_buf[0..(self.props.hdr_len - 288)]).is_err(){
            return Err(SasError::Read);
        }

        self.cached_page.extend_from_slice(&tmp_buf);
        if self.cached_page.len() != self.props.hdr_len{
            return Err(SasError::SasProperty("Sas7BDAT file seems to be truncated".to_string()));
        }
        self.props.page_count = self.read_int(PAGE_COUNT_OFFSET + align1, PAGE_COUNT_LENGTH)?;
        self.props.page_len = self.read_int(PAGE_SIZE_OFFSET + align1, PAGE_SIZE_LENGTH)?;
        self.read_bytes(SAS_RELEASE_OFFSET + total_align, SAS_RELEASE_LENGTH)?;
        self.sas_release = self.utf_8(&self.buf[0..SAS_RELEASE_LENGTH])?;

        self.read_bytes(SAS_SERVER_TYPE_OFFSET + total_align, SAS_SERVER_TYPE_LENGTH)?;
        self.server_type = self.utf_8(&self.buf[0..SAS_SERVER_TYPE_LENGTH])?;

        self.read_bytes(OS_VERSION_NUMBER_OFFSET + total_align, OS_VERSION_NUMBER_LENGTH)?;
        self.os_type = self.utf_8(&self.buf[0..OS_VERSION_NUMBER_LENGTH])?;

        self.read_bytes(OS_NAME_OFFSET + total_align, OS_NAME_LENGTH)?;
        if self.buf[0] != 0{
            self.os_name = self.utf_8(&self.buf[0..OS_NAME_LENGTH])?;
        }
        else {
            self.read_bytes(OS_MAKER_OFFSET + total_align, OS_MAKER_LENGTH)?;
            self.os_name = self.utf_8(&self.buf[0..OS_MAKER_LENGTH])?;
        }
        Ok(())
    }

    fn process_sub_hdr_ptrs(&mut self, off : usize, ptr_idx : usize) -> Result<SubHdrPtr, SasError>{
        let len = self.props.int_len;
        let sub_hdr_ptr_len = self.props.sub_hdr_ptr_len;
        let mut tot_off = off + sub_hdr_ptr_len * ptr_idx;
        let sub_hdr_off = self.read_int(tot_off, len)?;
        tot_off += len;
        let sub_hdr_len = self.read_int(tot_off, len)?;
        tot_off += len;
        let sub_hdr_compression = self.read_int(tot_off, 1)?;
        tot_off += 1;
        let sub_hdr_type = self.read_int(tot_off, 1)?;
        Ok(SubHdrPtr{
            off: sub_hdr_off , 
            len: sub_hdr_len, 
            compression: sub_hdr_compression, 
            ptype: sub_hdr_type})
    }

    fn read_sub_hdr_sig(&mut self, off : usize ) -> Result<Vec<u8>, SasError>{
        let len = self.props.int_len;
        self.read_bytes(off, len)?;
        let mut sub_hdr_sig : Vec<u8> = vec![0;len];
        sub_hdr_sig.copy_from_slice(&self.buf[0..len]);
        Ok(sub_hdr_sig)
    }

    fn get_sub_hdr_idx(&self, sig : Vec<u8>, compression: usize, ptype : usize) -> Result<usize, SasError> {
        match self.hdr_sig_map.get(&sig){
            Some(val) => Ok(*val),
            None => {
                let f = (compression == COMPRESSED_SUBHEADER_ID) || (compression == 0);
                if !self.compression.is_empty() && f && (ptype == COMPRESSED_SUBHEADER_TYPE){
                    Ok(DATA_SUBHDR_IDX)
                } else {
                    Err(SasError::SasProperty("unknown subheader signature".to_string()))
                }
            }
        }
    }
    fn process_row_size_sub_hdr(&mut self, off :usize, length : usize) -> Result<(), SasError>{
        let int_len = self.props.int_len;
        let mut lcs_off = off;
        let mut lcp_off = off;
        if self.u64 {
            lcs_off += 682;
            lcp_off += 706;
        } else {
            lcs_off += 354;
            lcp_off += 378;
        }
        self.props.row_len = self.read_int(off + ROW_LENGTH_OFFSET_MULTIPLIER * int_len, int_len)?;
        self.row_count = self.read_int(off + ROW_COUNT_OFFSET_MULTIPLIER * int_len, int_len)?;
        self.props.col_count_p1 = self.read_int(off + COL_COUNT_P1_MULTIPLIER * int_len, int_len)?;
        self.props.col_count_p2 = self.read_int(off + COL_COUNT_P2_MULTIPLIER * int_len, int_len)?;
        self.props.mix_page_row_cnt = self.read_int(off + ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER * int_len, int_len)?;
        self.props.lcs = self.read_int(lcs_off, 2)?;
        self.props.lcp = self.read_int(lcp_off, 2)?;
        Ok(())
    }

    fn process_col_size_sub_hdr(&mut self, mut off : usize, len : usize) -> Result<(), SasError> {
        let int_len = self.props.int_len;
        off += int_len;
        self.props.col_cnt = self.read_int(off, int_len)?;
        if self.props.col_count_p1 + self.props.col_count_p2 != self.props.col_cnt {
            return Err(SasError::SasProperty(format!("columnt count mismatch : {} + {} != {}", 
                                                     self.props.col_count_p1, self.props.col_count_p2, self.props.col_cnt)));
        }
        Ok(())
    }

    fn process_col_txt_sub_hdr(&mut self, mut off : usize, len : usize) -> Result<(), SasError> {
        off += self.props.int_len;
        //let txt_block_sz = len - self.props.int_len;
        let txt_block_sz = self.read_int(off, TEXT_BLOCK_SIZE_LENGTH)?;
        self.read_bytes(off, txt_block_sz)?;
        //println!("{:?}", &self.buf[0..txt_block_sz]);
        self.col_name_strings.push(self.buf[0..txt_block_sz].to_vec());
        if self.col_name_strings.len() == 1 {
            let col_name = &self.col_name_strings[0];
            let mut compression_literal = "".to_string();
            //if col_name.conta ("SASYZCRL"){
            if contains_bytes(col_name, "SASYZCRL"){
                println!("gotcha");
                compression_literal = "SASYZCRL".to_string();
            } else if contains_bytes(col_name, "SASYZCR2") {
                println!("gotcha");
                compression_literal = "SASYZCR2".to_string();
            }
            self.compression = compression_literal;
            off -= self.props.int_len;
            let mut off1 = off + 16;
            if self.u64{
                off1 += 4;
            }
            self.read_bytes(off1, self.props.lcp)?;
            compression_literal = self.utf_8(&self.buf[0..8])?;

            let x = compression_literal.as_str();
            if x.is_empty(){
                self.props.lcs = 0;
                off1 = off + 32;
                if self.u64{
                    off1 += 4;
                }
                self.read_bytes(off1, self.props.lcp)?;
                self.props.creator_proc = self.utf_8(&self.buf[0..self.props.lcp])?;

            } else if x == "SASYZCRL"{
                off1 = off + 40;
                if self.u64{
                    off1 += 4;
                }
                self.read_bytes(off1, self.props.lcp)?;
                self.props.creator_proc = self.utf_8(&self.buf[0..self.props.lcp])?;

            } else if self.props.lcs > 0 {
                self.props.lcp = 0;
                off1 = off + 16;
                if self.u64 {
                    off1 += 4;
                }
                self.read_bytes(off1, self.props.lcs)?;
                self.props.creator_proc = self.utf_8(&self.buf[0..self.props.lcs])?;

            }
        };
        Ok(())
        }

        fn process_col_name_sub_hdr(&mut self, mut off : usize, len : usize) -> Result<(), SasError> {
            let int_len = self.props.int_len;
            off += int_len;
            let col_name_ptr_cnt = (len - 2 * int_len - 12) / 8;
            for i in 0..col_name_ptr_cnt{
                let txt_sub_hdr = off + COLUMN_NAME_POINTER_LENGTH * (i + 1) + COLUMN_NAME_TEXT_SUBHEADER_OFFSET;
                let col_name_off = off + COLUMN_NAME_POINTER_LENGTH * (i + 1) + COLUMN_NAME_OFFSET_OFFSET;
                let col_name_len = off + COLUMN_NAME_POINTER_LENGTH * (i + 1) + COLUMN_NAME_LENGTH_OFFSET;
                let idx = self.read_int(txt_sub_hdr, COLUMN_NAME_TEXT_SUBHEADER_LENGTH)?;
                let col_off = self.read_int(col_name_off, COLUMN_NAME_OFFSET_LENGTH)?;
                let col_len = self.read_int(col_name_len, COLUMN_NAME_LENGTH_LENGTH)?;
                let name_str = &self.col_name_strings[idx];
                self.col_names.push(self.utf_8(&name_str[col_off .. col_off + col_len])?);
            }
            Ok(()) 
        }

        fn process_col_list_sub_hdr(&mut self, _off : usize, _len : usize) -> Result<(), SasError>{
            Ok(())
        }

        fn process_col_attr_sub_hdr(&mut self, off : usize, len : usize) -> Result<(), SasError> {
            let int_len = self.props.int_len;
            let col_attrs_vecs_cnt = (len - 2 * int_len - 12)/ (int_len + 8);
            for i in 0..col_attrs_vecs_cnt{
                let col_data_off = off + int_len + COLUMN_DATA_OFFSET_OFFSET + i * (int_len + 8);
                let col_data_len = off + 2 * int_len + COLUMN_DATA_LENGTH_OFFSET + i * (int_len + 8);
                let col_types = off + 2 * int_len + COLUMN_TYPE_OFFSET + i * (int_len + 8);
                let mut x = self.read_int(col_data_off, int_len)?;
                self.col_data_off.push(x);
                x = self.read_int(col_data_len, COLUMN_DATA_LENGTH_LENGTH)?;
                self.col_data_lens.push(x);
                x = self.read_int(col_types, COLUMN_TYPE_LENGTH)?;
                match x{
                    1 => self.col_types.push(SAS_NUM_TYPE),
                    _ => self.col_types.push(SAS_STRING_TYPE),
                }
            }
            Ok(())
        }

        fn process_format_sub_hdr(&mut self, off : usize) -> Result<(), SasError>{
            let int_len = self.props.int_len;
            let txt_sub_hdr_format = off + COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET + 3 * int_len;
            let col_format_off = off + COLUMN_FORMAT_OFFSET_OFFSET + 3 * int_len;
            let col_format_len = off+  COLUMN_FORMAT_LENGTH_OFFSET + 3 * int_len;
            let txt_sub_hdr_label = off + COLUMN_LABEL_TEXT_SUBHEADER_INDEX_OFFSET + 3 * int_len;
            let col_label_offset = off + COLUMN_LABEL_OFFSET_OFFSET + 3 * int_len;
            let col_label_len = off + COLUMN_LABEL_LENGTH_OFFSET + 3 * int_len;
            let mut format_idx = self.read_int(txt_sub_hdr_format, COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_LENGTH)?;
            format_idx = min(format_idx, self.col_name_strings.len() - 1);
            let format_start = self.read_int(col_format_off, COLUMN_FORMAT_OFFSET_LENGTH)?;
            let format_len = self.read_int(col_format_len, COLUMN_FORMAT_LENGTH_LENGTH)?;
            let mut label_idx = self.read_int(txt_sub_hdr_label, COLUMN_LABEL_TEXT_SUBHEADER_INDEX_LENGTH)?;
            label_idx = min(label_idx, self.col_name_strings.len() - 1);
            let label_start = self.read_int(col_label_offset, COLUMN_LABEL_OFFSET_LENGTH)?;
            let label_len = self.read_int(col_label_len, COLUMN_LABEL_LENGTH_LENGTH)?;
            let label_names = &self.col_name_strings[label_idx];
            let col_label = self.utf_8(&label_names[label_start .. label_start + label_len])?;
            let format_names = &self.col_name_strings[format_idx];
            let col_format = self.utf_8(&format_names[format_start..format_start + format_len])?;
            let cur_col_number = self.cols.len(); 

            let col = Col{
                col_id: cur_col_number,
                name: self.col_names[cur_col_number].clone(),
                label: col_label.clone(),
                fmt: col_format.clone(), 
                ctype: self.col_types[cur_col_number],
                len: self.col_data_lens[cur_col_number],
            };
            self.col_labels.push(col_label);
            self.col_formats.push(col_format);
            self.cols.push(col);
            Ok(())
        }
        fn row_count(&self) -> usize {
            self.row_count
        }
        fn col_names(&self) -> &Vec<String>{
            &self.col_names
        }
        fn col_label(&self) -> &Vec<String>{
            &self.col_labels
        }
        fn col_types(&self) -> &Vec<u16>{
            &self.col_types
        }
        fn parse_metadata(&mut self) -> Result<(), SasError> {
            loop {
                if self.buf_rdr.read_exact(&mut self.cached_page).is_err(){
                    return Err(SasError::Read);
                }
                match self.process_page_meta() {
                    Ok(done) => if done {
                        break;
                    }
                    Err(val) => return Err(val),
                }
            };
            Ok(())
        }

        fn is_page_metamix_amd(&self, page_type : isize) -> bool{
            matches!(page_type, PAGE_META_TYPE | 512 | 640 | PAGE_AMD_TYPE)
        }

        fn is_page_mix_type(&self, val : isize) -> bool{
            matches!(val, 512 | 640)
        }

        fn is_page_mix_data_type(&self, val : isize) -> bool {
            matches!(val, 512 | 640 | 256)
        }

        fn check_page_type(&self, cur_page : isize) -> bool {
            !matches!(cur_page, PAGE_META_TYPE | PAGE_DATA_TYPE | 512 | 640)
        }

        fn process_page_meta(&mut self) -> Result<bool, SasError> {
            self.read_page_hdr()?;
            if self.is_page_metamix_amd(self.cur_page_type){
                self.process_page_metadata()?;
            }
            Ok(self.is_page_mix_data_type(self.cur_page_type) || !self.cur_page_data_sub_hdr_pointers.is_empty())
        }

        fn process_sub_hdr_counts(&self, _off : usize, _len : usize) -> Result<(), SasError>{
            Ok(())
        }

        fn process_sub_hdr(&mut self, sub_hdr_idx : usize, ptr : SubHdrPtr) -> Result<(), SasError>{
            let off = ptr.off;
            let len = ptr.len;

            match sub_hdr_idx {
                ROW_SIZE_IDX => self.process_row_size_sub_hdr(off, len),
                COL_SIZE_IDX => self.process_col_size_sub_hdr(off, len),
                COL_TEXT_IDX => self.process_col_txt_sub_hdr(off, len),
                COL_NAME_IDX => self.process_col_name_sub_hdr(off, len),
                COL_ATTR_IDX => self.process_col_attr_sub_hdr(off, len),
                FMT_AND_LABEL_IDX => self.process_format_sub_hdr(off),
                COL_LIST_IDX => self.process_col_list_sub_hdr(off, len),
                SUB_HDR_CNT_IDX => self.process_sub_hdr_counts(off, len),
                DATA_SUBHDR_IDX => {
                    self.cur_page_data_sub_hdr_pointers.push(ptr);
                    Ok(())
                }
                _ => Err(SasError::SasProperty("Invalid processor index type".to_string())),
            }
        }

        fn process_page_metadata(&mut self) -> Result<(), SasError>{
            let bit_off = self.props.page_bit_off;
            for i in 0..self.cur_page_sub_hdr_count{
                let ptr = self.process_sub_hdr_ptrs(SUBHEADER_POINTERS_OFFSET + bit_off, i)?;
                if ptr.len == 0 || ptr.compression == TRUNCATED_SUBHEADER_ID {
                    continue;
                }
                let sub_hdr_sig = self.read_sub_hdr_sig(ptr.off)?;
                let sub_hdr_idx = self.get_sub_hdr_idx(sub_hdr_sig, ptr.compression, ptr.ptype)?;
                self.process_sub_hdr(sub_hdr_idx, ptr)?;
            }
            Ok(())
        }

        fn ensure_buf_len(&mut self, len : usize){
            if self.buf.len() < len {
                self.buf.resize(2 * len, 0);
            }
        }

        fn read_bytes(&mut self, off : usize, len : usize) -> Result<(), SasError> {
            self.ensure_buf_len(len);
            if self.cached_page.is_empty(){
                self.buf_rdr.seek(SeekFrom::Start(off.try_into().unwrap())).expect("Failed to seek file");
                if self.buf_rdr.read_exact(&mut self.buf[0..len]).is_err(){
                    return Err(SasError::Read);
                }
            } else {
                if off + len > self.cached_page.len(){
                    return Err(SasError::Read);
                }
                self.buf[0..len].copy_from_slice(&self.cached_page[off..(off+len)]);
            }
            Ok(())
        }

        fn read_float(&self, off : usize, w : usize) -> f64{
            match self.byte_order {
                Endian::Big => f64::from_be_bytes(self.buf[off..(off + w)].try_into().unwrap()),
                Endian::Little => f64::from_le_bytes(self.buf[off..(off + w)].try_into().unwrap()),
            }
        }

        fn read_int_from_buf(&self, w : usize) -> Result<usize, SasError> {
            match w {
                1 => match (i8::from_bytes(&self.buf, 0, 1, &self.byte_order)).try_into(){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                2 => match (i16::from_bytes(&self.buf, 0, 2, &self.byte_order)).try_into() {
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                4 => match (i32::from_bytes(&self.buf, 0, 4, &self.byte_order)).try_into(){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                8 => match (i64::from_bytes(&self.buf, 0, 8, &self.byte_order)).try_into(){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                _ => Err(SasError::TypeConversion),
            }
        }

        fn read_int(&mut self, off :usize, w :usize) -> Result<usize, SasError> {
            self.read_bytes(off, w)?;
            self.read_int_from_buf(w)
        }

        fn read_signed_int_from_buf(&self, w : usize) -> Result<isize, SasError> {

            match w {
                1 => match (i8::from_bytes(&self.buf, 0, 1, &self.byte_order)).try_into(){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                2 => match (i16::from_bytes(&self.buf, 0, 2, &self.byte_order)).try_into() {
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                4 => match (i32::from_bytes(&self.buf, 0, 4, &self.byte_order)).try_into(){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                8 => match (i64::from_bytes(&self.buf, 0, 8, &self.byte_order)).try_into(){
                    Ok(val) => Ok(val),
                    Err(_) => Err(SasError::TypeConversion),
                }
                _ => panic!("Invalid int width"),
            }
        }

        fn read_signed_int(&mut self, off :usize, w :usize) -> Result<isize, SasError> {
            self.read_bytes(off, w)?;
            self.read_signed_int_from_buf(w)
        }

        fn process_byte_array_with_data(&mut self, off : usize, len : usize) -> Result<(), SasError>{
            let mut src : Vec<u8> = Vec::new();
            if !self.compression.is_empty() && len < self.props.row_len {
                let decomp = self.get_decompressor();
                match decomp {
                    Some(f) => {
                        match f(self.props.row_len, &self.cached_page[off .. off + len]) {
                            Ok(vec) => {
                                src = vec;
                            }
                            Err(val) => return Err(val),
                        }
                    }
                    None => {
                        return Err(SasError::SasProperty("Compressor specified, but not found".to_string()));
                    }
                }
            } else {
                if off + len > self.cached_page.len() {
                    let old_page = self.cached_page.clone();
                    match self.read_next_page() {
                        Ok(true) => self.cached_page.extend_from_slice(&old_page),
                        Ok(false) => return Err(SasError::SasProperty("Error reading next page!".to_string())),
                        Err(_) => return Err(SasError::SasProperty("Error reading next page!".to_string())),
                    }
                }
                src = self.cached_page[off .. off + len].to_vec();
            };

            for j in 0..self.props.col_cnt{
                let len = self.col_data_lens[j];
                if len == 0{
                    break;
                }

                let start = self.col_data_off[j];
                let end = start + len;
                let tmp = &src[start..end];
                //Handle numeric types: 
                if self.cols[j].ctype == SAS_NUM_TYPE {
                    //First perform float conversion
                    self.float_buf = [0;8];
                    let num = 
                        match self.byte_order {
                            Endian::Little => {
                                let m = 8 - len;
                                self.float_buf[m .. 8].copy_from_slice(tmp);
                                f64::from_le_bytes(self.float_buf)
                            }
                            Endian::Big => {
                                self.float_buf[0 .. len].copy_from_slice(tmp);
                                f64::from_be_bytes(self.float_buf)
                            }
                        };

                    //Check which kind of datetype is given, then convert according 
                    //to definition.
                    if self.col_formats[j] == "DATE"|| self.col_formats[j] == "DDMMYY"{
                        let new = self.date_base + Duration::days(num as i64);
                        self.row_vals[j] = SasVal::Date(new.date_naive());
                    } else if self.col_formats[j] == "DATETIME"{
                        let new = self.date_base + Duration::seconds(num as i64);
                        self.row_vals[j] = SasVal::DateTime(new);
                    } else {
                        self.row_vals[j] = SasVal::Numeric(num);
                    }

                    //Handle String types
                } else {
                    let mut st = self.utf_8(tmp)?;
                    if self.trim_strings{
                        st = st.trim_end_matches(&['\u{0000}', '\u{0020}']).to_string();
                    }
                    self.row_vals[j] = SasVal::Text(st);
                    //println!("{st}");
                }
            }

            self.cur_row_on_page_idx += 1;
            self.cur_row_in_chunk_idx += 1;
            self.cur_row_in_file_idx += 1;
            Ok(())
        }

        fn read_page_hdr(&mut self) -> Result<(), SasError>{
            let bit_off = self.props.page_bit_off;
            self.cached_page.len();
            self.cur_page_block_count = self.read_int(BLOCK_COUNT_OFFSET + bit_off, BLOCK_COUNT_LENGTH)?;
            self.cur_page_sub_hdr_count = self.read_int(SUBHEADER_COUNT_OFFSET + bit_off, SUBHEADER_COUNT_LENGTH)?;
            //TODO factor out read_signed_int
            self.cur_page_type = self.read_signed_int(PAGE_TYPE_OFFSET + bit_off, PAGE_TYPE_LENGTH)?;
            Ok(())
        }

        fn read_next_page(&mut self) -> Result<bool, SasError>{
            self.cur_page_data_sub_hdr_pointers = Vec::with_capacity(10);
            self.cached_page = vec![0;self.props.page_len];
            let n = self.buf_rdr.read_exact(&mut self.cached_page);
            match n {
                Ok(()) => (),
                //number of red bytes smaller than cached_page length
                Err(er) if er.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(true);
                }
                Err(_) => return Err(SasError::Read),
            }

            self.read_page_hdr()?;
            if self.cur_page_type == PAGE_META_TYPE {
                self.process_page_metadata()?;
            }
            if self.check_page_type(self.cur_page_type) {
                return self.read_next_page();
            }
            Ok(false)
        }

        pub fn read_line(&mut self) -> Result<bool, SasError> {
            let bit_off = self.props.page_bit_off;
            let sub_hdr_ptr_len = self.props.sub_hdr_ptr_len;

            if self.cached_page.is_empty() {
                self.buf_rdr.seek(SeekFrom::Start(self.props.hdr_len.try_into().unwrap())).expect("Could not read page!");
                self.read_next_page()?;
            }

            loop {
                if self.cur_page_type == PAGE_META_TYPE {
                    if self.cur_row_on_page_idx >= self.cur_page_data_sub_hdr_pointers.len() {
                        match self.read_next_page(){
                            Ok(true) => return Ok(false),
                            Ok(false) =>{ 
                                self.cur_row_on_page_idx = 0;
                                continue;
                            }
                            Err(val) => return Err(val),
                        }
                    }
                    let cur_sub_hdr_ptr = &self.cur_page_data_sub_hdr_pointers[self.cur_row_on_page_idx];
                    match self.process_byte_array_with_data(cur_sub_hdr_ptr.off, cur_sub_hdr_ptr.len){
                        Ok(()) => return Ok(true),
                        Err(val) => return Err(val),
                    }
                } else if self.is_page_mix_type(self.cur_page_type) {
                    let mut align_corr = bit_off + SUBHEADER_POINTERS_OFFSET +
                        self.cur_page_sub_hdr_count * sub_hdr_ptr_len % 8;
                    if self.no_align_correction {
                        align_corr = 0;
                    }
                    let off = bit_off + SUBHEADER_POINTERS_OFFSET + 
                        self.cur_page_sub_hdr_count * sub_hdr_ptr_len +
                        self.cur_row_on_page_idx * self.props.row_len +
                        align_corr;
                    if self.process_byte_array_with_data(off, self.props.row_len).is_err() {
                        return Err(SasError::SasProperty("Could not process byte array".to_string()));
                    }
                    if self.cur_row_on_page_idx == min(self.row_count, self.props.mix_page_row_cnt){
                        match self.read_next_page(){
                            Ok(true) => return Ok(false),
                            Err(val) => return Err(val),
                            Ok(false) => {
                                self.cur_row_on_page_idx = 0;
                            }
                        }
                    }
                    return Ok(true);
                } else if self.cur_page_type == PAGE_DATA_TYPE {
                    if self.process_byte_array_with_data(
                        bit_off + SUBHEADER_POINTERS_OFFSET + self.cur_row_on_page_idx * self.props.row_len,
                        self.props.row_len).is_err(){
                        return Err(SasError::SasProperty("Could not process byte array".to_string()));
                    }
                    if self.cur_row_on_page_idx == self.cur_page_block_count {
                        match self.read_next_page(){
                            Ok(true) => return Ok(false),
                            Err(val) => return Err(val),
                            Ok(false) => {
                                self.cur_row_on_page_idx = 0;
                            }
                        }
                    }
                    return Ok(true);
                } else {
                    return Err(SasError::SasProperty(format!("unknown page type : {}", self.cur_page_type)));
                }
            }
        }

        pub fn new(reader : std::io::BufReader<R>) -> Result<SAS7bdat<R>, SasError> {
            let mut sas = SAS7bdat{
                date_base : Utc.with_ymd_and_hms(1960,1,1,0,0,0).unwrap(),
                float_buf : [0;8],
                row_vals : Vec::new(),
                col_formats : Vec::default(),
                trim_strings : false,
                no_align_correction : false,
                date_created : 0.,
                date_modified : 0.,
                name : String::default(),
                platform : String::default(),
                sas_release : String::default(),
                server_type : String::default(),
                os_type : String::default(),
                os_name : String::default(),
                file_type : String::default(),
                file_encoding : String::default(),
                u64 : false,
                byte_order : Endian::default(),
                compression : String::default(),
                text_decoder : Encodings::MultiByte(UTF8Encoding),
                row_count : usize::default(),
                col_types : Vec::default(),
                col_labels : Vec::default(),
                col_names : Vec::default(),
                buf : Vec::default(),
                buf_rdr : reader,
                cached_page : Vec::default(),
                cur_page_type : isize::default(),
                cur_page_block_count : usize::default(),
                cur_page_sub_hdr_count : usize::default(),
                cur_row_in_file_idx : usize::default(),
                cur_row_on_page_idx : usize::default(),
                cur_page_data_sub_hdr_pointers : Vec::default(),
                cur_row_in_chunk_idx : usize::default(),
                col_name_strings : Vec::default(),
                col_data_off : Vec::default(),
                col_data_lens : Vec::default(),
                cols : Vec::default(),
                props : SasProperties::default(),
                encoding_map : get_encoding_map(),
                hdr_sig_map : get_hdr_sig_map(),
            };
            sas.get_properties()?;
            sas.cached_page = vec![0;sas.props.page_len];
            sas.parse_metadata()?;
            sas.row_vals = vec![SasVal::Numeric(0.0);sas.props.col_cnt];
            //println!("col count = {}", sas.cur_row_in_file_idx);
            Ok(sas)
        }
    }


    fn get_encoding_map() -> HashMap<usize, &'static str>{
        HashMap::from([
                      (29, "latin1"),
                      (20, "utf-8"),
                      (33, "cyrillic"),
                      (60, "wlatin2"),//windows1250
                      (61, "wcyrillic"),
                      (62, "wlatin1"),//windows1252
                      (90, "ebcdic870")])
    }

    fn rle_decompress(res_len : usize, input : &[u8]) -> Result<Vec<u8>, SasError>{
        let mut res : Vec<u8> = Vec::with_capacity(res_len);
        let mut inbuf = input;
        while !inbuf.is_empty() {
            let control_byte = inbuf[0] & 0xF0;
            let end_of_first_byte = usize::from(inbuf[0] & 0x0F);
            inbuf = &inbuf[1..];
            match control_byte {
                0x00 => {
                    //if end_of_first_byte != 0 {
                    //    return Err(SasError::UnexpectedEndOfControlByte);
                    //}
                    //let nbytes = usize::from(inbuf[0]) + 64;
                    println!("got you a 0");
                    let nbytes = usize::from(inbuf[0]) + 64 + end_of_first_byte * 256;
                    inbuf = &inbuf[1..];  
                    res.extend_from_slice(&inbuf[0..nbytes]);
                    inbuf = &inbuf[nbytes..];
                }
                0x40 => {
                    let nbytes = end_of_first_byte * 256 + usize::from(inbuf[0]) + 18;
                    inbuf = &inbuf[1..];
                    for _ in 0..nbytes{
                        res.push(inbuf[0]);
                    }
                    inbuf = &inbuf[1..];
                }
                0x60 => {
                    let nbytes = end_of_first_byte * 256 + usize::from(inbuf[0]) + 17;
                    inbuf = &inbuf[1..];
                    for _ in 0..nbytes{
                        res.push(0x20);
                    }
                }
                0x70 => {
                    let nbytes = end_of_first_byte * 256 + usize::from(inbuf[0]) + 17;
                    inbuf = &inbuf[1..];
                    for _ in 0..nbytes{
                        res.push(0x00);
                    }
                }
                0x80 => {
                    let nbytes = end_of_first_byte + 1;
                    res.extend_from_slice(&inbuf[0..nbytes]);
                    inbuf = &inbuf[nbytes..];
                }
                0x90 => {
                    let nbytes = end_of_first_byte + 17;
                    res.extend_from_slice(&inbuf[0..nbytes]);
                    inbuf = &inbuf[nbytes..];
                }
                0xA0 => {
                    let nbytes = end_of_first_byte + 33;
                    res.extend_from_slice(&inbuf[0..nbytes]);
                    inbuf = &inbuf[nbytes..];
                }
                0xB0 => {
                    let nbytes = end_of_first_byte + 49;
                    res.extend_from_slice(&inbuf[0..nbytes]);
                    inbuf = &inbuf[nbytes..];

                }
                0xC0 => {
                    let nbytes = end_of_first_byte + 3;
                    let x = inbuf[0];
                    inbuf = &inbuf[1..];
                    for _ in 0..nbytes{
                        res.push(x);
                    }
                }
                0xD0 => {
                    let nbytes = end_of_first_byte + 2;
                    for _ in 0..nbytes{
                        res.push(0x40);
                    }
                }
                0xE0 => {
                    let nbytes = end_of_first_byte + 2;
                    for _ in 0..nbytes{
                        res.push(0x20);
                    }
                }
                0xF0 => {
                    let nbytes = end_of_first_byte + 2;
                    for _ in 0..nbytes{
                        res.push(0x00);
                    }
                }
                _ => { return Err(SasError::ControlByte); }
            }
        }
        if res.len() != res_len{
            println!("rle");
            println!("cur_len {} is not equalt to requested_len {res_len}", res.len());
            return Err(SasError::BufLen);
        }
        Ok(res)
    }

    fn rdc_decompress(res_len : usize, inbuf : &[u8]) -> Result<Vec<u8>, SasError>{
        let mut ctrl_bits : u16 = 0;
        let mut ctrl_mask : u16 = 0;
        let mut cmd : u8;
        let mut ofs : u16;
        let mut cnt : u16;
        let mut inbuf_pos : usize = 0;
        let mut res : Vec<u8> = Vec::with_capacity(res_len);

        while inbuf_pos < inbuf.len(){
            ctrl_mask >>= 1;
            if ctrl_mask == 0{
                ctrl_bits = u16::from(inbuf[inbuf_pos]) << 8 + u16::from(inbuf[inbuf_pos+1]);
                inbuf_pos += 2;
                ctrl_mask = 0x8000;
            }
            if (ctrl_bits & ctrl_mask) == 0 {
                res.push(inbuf[inbuf_pos]);
                inbuf_pos += 1;
                continue;
            }
            cmd = (inbuf[inbuf_pos] >> 4) & 0x0F;
            cnt = u16::from(inbuf[inbuf_pos] & 0x0F);
            inbuf_pos += 1;

            match cmd{
                0 => {
                    cnt += 3;
                    for _ in 0..usize::from(cnt){
                        res.push(inbuf[inbuf_pos]);
                    }
                    inbuf_pos += 1;
                }
                1 => {
                    cnt += u16::from(inbuf[inbuf_pos]) << 4;
                    cnt += 19;
                    inbuf_pos += 1;
                    for _ in 0..usize::from(cnt){
                        res.push(inbuf[inbuf_pos]);
                    }
                    inbuf_pos += 1;
                }
                2 => {
                    ofs = cnt + 3;
                    ofs += u16::from(inbuf[inbuf_pos]) << 4;
                    inbuf_pos += 1;
                    cnt = u16::from(inbuf[inbuf_pos]);
                    inbuf_pos += 1;
                    cnt += 16;
                    res.extend_from_within((res.len() -usize::from(ofs))..(res.len()-usize::from(ofs)+usize::from(cnt)));
                }
                3..=16 => {
                    ofs = cnt + 3;
                    ofs += u16::from(inbuf[inbuf_pos]) << 4;
                    inbuf_pos += 1;
                    res.extend_from_within((res.len() - usize::from(ofs))..
                                           (res.len() - usize::from(ofs) + usize::from(cmd)));
                }
                _ => {
                    return Err(SasError::Cmd);
                }

            }
        }

        if res.len() != res_len {
            return Err(SasError::BufLen);
        }

        Ok(res)
    }
