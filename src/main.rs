mod connectbk;
use connectbk::connectbk;
use rusqlite::{Connection, Result};
use std::path::{Path};
use std::io::{Write, BufRead, BufReader};
use std::fs::File;
use std::env;
use std::process::Command as stdCommand;
use std::time::Instant as timeInstant;
use chrono::Local;
/*
#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
} */
#[derive(Debug)]
struct Outpt {
    name: String,
}
#[derive(Debug)]
struct Bkup {
      rowid: u64,
      refname: String,
      filename: String,
      dirname: String,
      filesize: u64,
      filedate: String,
      md5sum: Option<String>,
      locations: Option<String>,
      notes: Option<String>,
}

#[derive(Debug)]
struct Hd {
      rowid: u64,
      filename: String,
      filesize: u64,
      filedate: String,
      dirname: String,
      refname: String,
      md5sum: Option<String>,
      locations: Option<String>,
      notes: Option<String>,
}


fn main() -> Result<()> {
    let mut bolok = true;
    let mut rows_num: u64 = 0;
    let mut exrows_num: u64 = 0;
    let mut parm1dir = String::new();
    let mut parm2dir = String::new();
    let mut parm3dir = String::new();
    let mut vecexcludef: Vec<String> = Vec::new();
    let mut vecexcluded: Vec<String> = Vec::new();
    let mut outseq: u32 = 1;
    let mut linestrtnum: u64 = 1;

    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        println!(" no input parameters");
        bolok = false;
    } else {
        println!("The first argument is {}", args[1]);
        if args.len() < 3 {
            println!("The Only first argument and no other arguments");
            bolok = false;
        } else {
            println!("The second argument is {}", args[2]);
            if args.len() < 4 {
                println!("The Only first and second arguments and no other arguments");
                bolok = false;
            } else {
                println!("The third argument is {}", args[3]);
                if Path::new(&args[1]).exists() {
                    println!("The first argument {} exists", args[1]);
                    parm1dir = args[1].to_string();
                    let conn1 = Connection::open(parm1dir.clone()).unwrap();
                    if let Err(e) = connectbk(&conn1) {
                        println!("data base for backup error: {}", e);
                        bolok = false;
                    } else {
                        println!("data base good for backup");
                    }
                } else {
                    println!("The first argument {} does not exist", args[1]);
                    bolok = false;
                }
                if !Path::new(&args[2]).exists() {
                    println!("The second argument {} does not exist", args[2]);
                    bolok = false;
                } else {
                    println!("The second argument {} exists", args[2]);
                    parm2dir = args[2].to_string();
                    let outputx = stdCommand::new("wc")
                         .arg("-l")
                         .arg(&parm2dir)
                         .output()
                         .expect("failed to execute process");
                    let stroutx = String::from_utf8_lossy(&outputx.stdout);
                    let vecout: Vec<&str> = stroutx.split(" ").collect();
                    let numlinesx: i64 = vecout[0].parse().unwrap_or(-9999);
                    if numlinesx == -9999 {
                        println!("size of {} is invalid for wc -l command call", vecout[0]);
                        bolok = false;
                    } else {
                        rows_num = numlinesx as u64;
                        if rows_num < 2 {
                            println!("size of {} is less than 2 for {}", rows_num, parm2dir);
                            bolok = false;
                        } else {
                            let file = File::open(parm2dir.clone()).unwrap();
                            let mut reader = BufReader::new(file);
                            let mut linehd = String::new();
                            let mut linenum: u64 = 0;
                            bolok = false;
                            loop {
                               match reader.read_line(&mut linehd) {
                                  Ok(bytes_read) => {
                                     // EOF: save last file address to restart from this address for next run
                                     if bytes_read == 0 {
                                         println!("bytes_read == 0 for {}", parm2dir);
                                         break;
                                     }
                                     linenum = linenum + 1;
                                     if linenum == 1 {
                                         if linehd.trim().to_string() == "filename|filesize|filedate|dirname|refname|md5sum|locations|notes".to_string() {
                                             println!("hd file is ok with size of {} rows", rows_num);
                                             bolok = true;
                                             break;
                                         } else {
                                             println!("first line of hd file is not valid: {}", linehd);
                                             break;
                                         }
                                     } else {
                                         println!("linenum after 1 for {}", parm2dir);
                                         break;
                                     }
                                  }
                                  Err(err) => {  
                                     println!("error of {} reading {}", err, parm2dir);
                                     break;
                                  }
                               };
                            }
                        }
                    }
                }
                if !Path::new(&args[3]).exists() {
                    println!("The third argument {} does not exist", args[3]);
                    bolok = false;
                } else {
                    println!("The third argument {} exists", args[3]);
                    parm3dir = args[3].to_string();
                    let outputy = stdCommand::new("wc")
                         .arg("-l")
                         .arg(&parm3dir)
                         .output()
                         .expect("failed to execute process");
                    let strouty = String::from_utf8_lossy(&outputy.stdout);
                    let vecouty: Vec<&str> = strouty.split(" ").collect();
                    let numlinesy: i64 = vecouty[0].parse().unwrap_or(-9999);
                    if numlinesy == -9999 {
                        println!("size of {} is invalid for wc -l command call", vecouty[0]);
                        bolok = false;
                    } else {
                        exrows_num = numlinesy as u64;
                        if exrows_num < 2 {
                            println!("size of {} is less than 2 for {}", exrows_num, parm3dir);
                            bolok = false;
                        } else {
                            let filey = File::open(parm3dir.clone()).unwrap();
                            let mut readery = BufReader::new(filey);
                            let mut lineex = String::new();
                            let mut linenumy: u64 = 0;
                            bolok = false;
                            loop {
                               match readery.read_line(&mut lineex) {
                                  Ok(bytes_read) => {
                                     // EOF: save last file address to restart from this address for next run
                                     if bytes_read == 0 {
                                         break;
                                     }
                                     linenumy = linenumy + 1;
                                     if linenumy == 1 {
                                         if lineex.trim().to_string() == "exclude file".to_string() {
                                             println!("exclude file is ok");
                                             bolok = true;
                                         } else {
                                             println!("first line of exclude file is not valid: {}", lineex);
                                         }
                                     } else {
                                         break;
                                     }
                                  }
                                  Err(err) => {  
                                     break;
                                  }
                               };
                            }
                        }
                    }
                    if args.len() > 4 {
                       let arg4 = args[4].to_string();
                       let numarg4: i64 = arg4.parse().unwrap_or(-9999);
                       if numarg4 < 2 {
                           println!("argument 4 is not valid value: {}", arg4);
                           bolok = false;
                       } else {
                           linestrtnum = numarg4 as u64;
                       }
                    }
                }
            }
        }
    }
    if bolok {
        let fileex = File::open(parm3dir.clone()).unwrap();
        let mut readerex = BufReader::new(fileex);
        let mut lineex = String::new();
        let mut lineexnum: u64 = 0;
        loop {
              match readerex.read_line(&mut lineex) {
                 Ok(bytes_read) => {
                 // EOF: save last file address to restart from this address for next run
                     if bytes_read == 0 {
                         break;
                     }
                     lineexnum = lineexnum + 1;
                     if lineexnum > 1 {
                         let excl: String = lineex.trim().to_string();
                         if excl.len() < 3 {
                             println!("exclusion less than 3 characters: {}", excl);
                             bolok = false;
                             break;
                         } else {
                             let exclv: String = excl[2..].to_string();
                             println!("exclusion value:-{}-", exclv);
                             if excl[..2].to_string() == "d ".to_string() {
                                 vecexcluded.push(exclv);
                             } else if excl[..2].to_string() == "f ".to_string() {
                                 vecexcludef.push(exclv);
                             } else {
                                 println!("exclusion invalid first two characters {}", excl);
                                 bolok = false;
                                 break;
                             }
                         }   
                     }
                     lineex.clear();
                 }
                 Err(err) => {
                     println!("error {} when reading exclusion file", err);
                     bolok = false;   
                     break;
                 }
              };
        }
        if lineexnum < 2 {
            println!("exclusion file {} has no records", parm3dir);
            bolok = false;
        } else {
            lineexnum = lineexnum - 1;
            println!("exclusion file {} has {} records", parm3dir, lineexnum);
        }
    }
    if bolok {
        let mut more1out: String = format!("./more1{:02}.excout", outseq);
        let mut just1out: String = format!("./just1{:02}.neout", outseq);
        let mut excludout: String = format!("./excluded{:02}.excout", outseq);
        let mut nobkupout: String = format!("./nobkup{:02}.neout", outseq);
        let mut errout: String = format!("./generrors{:02}.errout", outseq);
        loop {
               if !Path::new(&errout).exists() && !Path::new(&more1out).exists() && !Path::new(&just1out).exists()
                  && !Path::new(&excludout).exists() && !Path::new(&nobkupout).exists() {
                   break;
               } else {
                   outseq = outseq + 1;
                   more1out = format!("./more1{:02}.excout", outseq);
                   just1out = format!("./just1{:02}.neout", outseq);
                   excludout = format!("./excluded{:02}.excout", outseq);
                   nobkupout = format!("./nobkup{:02}.neout", outseq);
                   errout = format!("./generrors{:02}.errout", outseq);
               }
        }          
        let conndb = Connection::open(parm1dir.clone()).unwrap();
        let mut excludefile = File::create(excludout).unwrap();
        let mut nobkupfile = File::create(nobkupout).unwrap();
        let mut more1file = File::create(more1out).unwrap();
        let mut just1file = File::create(just1out).unwrap();
        let mut errfile = File::create(errout).unwrap();
        let filex = File::open(parm2dir.clone()).unwrap();
        let mut readerx = BufReader::new(filex);
        let mut linex = String::new();
        let mut line1000: u64 = 0;
        let mut linenumx: u64 = 0;
        let start_time = timeInstant::now();

        loop {
              match readerx.read_line(&mut linex) {
                 Ok(bytes_read) => {
                 // EOF: save last file address to restart from this address for next run
                     if bytes_read == 0 {
                         break;
                     }
                     line1000 = line1000 + 1;
                     linenumx = linenumx + 1;
                     let mut bolin = false;
                     if linenumx > linestrtnum {
                         if line1000 > 500 {
                             let diffy = start_time.elapsed();
                             let minsy: f64 = diffy.as_secs() as f64/60 as f64;
                             let dateyy = Local::now();
                             println!("line number {} records elapsed time {:.1} mins at {}", linenumx, minsy, dateyy.format("%H:%M:%S"));
                             line1000 = 0;
                         }
                         let vecline: Vec<&str> = linex.split("|").collect();
                         let inptdir = vecline[3].to_string();
                         let mut inptfilenm: String = vecline[0].to_string();
                         if inptfilenm[..1].to_string() == '"'.to_string() {
                             inptfilenm = inptfilenm[1..(inptfilenm.len()-1)].to_string();
                             bolin = true;
                         }
                         let mut bnotex = true;
                         for strexclf in &vecexcludef {
                              if inptfilenm.contains(strexclf) {
                                  bnotex = false;
                                  let stroutput = format!("{}|{}", linex, linenumx);
                                  writeln!(&mut excludefile, "{}", stroutput).unwrap();
                                  break;
                              }
                         }
                         for strexcld in &vecexcluded {
                              if inptdir.contains(strexcld) {
                                  bnotex = false;
                                  let stroutput = format!("{}|{}", linex, linenumx);
                                  writeln!(&mut excludefile, "{}", stroutput).unwrap();
                                  break;
                              }
                         }
                         if bnotex {
//                             let inptfilesz: String = vecline[1].to_string();
                             let inptfilemd5: String = vecline[5].to_string();
                             match conndb.prepare("SELECT  rowid, refname, filename, dirname, filesize, filedate, md5sum, locations, notes
                                    FROM blubackup
                                    WHERE filename = :fil") {
                                     Err(err) => {
                                         writeln!(&mut errfile, "err {} in sql prepare call for file {}", err, inptfilenm).unwrap();
                                     }
                                     Ok(mut stmt) => {
                                         match stmt.query_map(&[(":fil", &inptfilenm)], |row| {
                                           Ok(Bkup {
                                             rowid: row.get(0)?,
                                             refname: row.get(1)?,
                                             filename: row.get(2)?,
                                             dirname: row.get(3)?,
                                             filesize: row.get(4)?,
                                             filedate: row.get(5)?,
                                             md5sum: row.get(6)?,
                                             locations: row.get(7)?,
                                             notes: row.get(8)?,
                                           })
                                          })
                                         {
                                          Err(err) => {
                                             writeln!(&mut errfile, "err {} in sql query for file {}", err, inptfilenm).unwrap();
                                          }
                                          Ok(bk_iter) => {
                                             let mut numentries = 0;
                                             let mut nummatch = 0;
                                             for bk in bk_iter {
                                                let mut filemd5 = String::new();
                                                numentries = numentries + 1;
                                                let bki = bk.unwrap();
                                                if bki.md5sum == None {
                                                   filemd5 = "----".to_string();
                                                } else {
                                                   let filemd5x = bki.md5sum.expect("REASON").to_string();
                                                   filemd5 = filemd5x[..32].to_string();
                                                   if filemd5 == inptfilemd5 {
                                                       nummatch = nummatch + 1;
                                                   }
                                                }
                                             }
                                             if nummatch < 1 {
                                                 let stroutput: String;
 //                                                if bolin {
 //                                                   stroutput = format!("{} -- -{}-", linex, inptfilenm);
//                                                 } else {
                                                    stroutput = format!("{}|{}", linex, linenumx);
//                                                 }
                                                 writeln!(&mut nobkupfile, "{}", stroutput).unwrap();
                                             } else if nummatch < 2 {
                                                 let stroutput = format!("{}|{}", linex, linenumx);
                                                 writeln!(&mut just1file, "{}", stroutput).unwrap();
                                             } else {
                                                 let stroutput = format!("{}|{}", linex, linenumx);
                                                 writeln!(&mut more1file, "{}", stroutput).unwrap();
                                             }
                                          }
                                         }
                                     }
                             }
                         }
                     }
                     linex.clear();
 
                 }
                 Err(err) => {
                     bolok = false;   
                     break;
                 }
              };
        }
        println!("{} files", linenumx);
    }
    Ok(())
}
