//! NBD trace replay client.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;

use portalbd_trace::{NbdOp, OpTrace, load_trace};

#[derive(Parser, Debug)]
#[command(name = "trace-replay", about = "Replay traces against NBD server")]
struct Args {
    /// Path to trace.json file
    trace: PathBuf,

    /// NBD server address
    #[arg(short, long, default_value = "127.0.0.1:10809")]
    server: String,

    /// Number of replay iterations
    #[arg(short, long, default_value = "1")]
    iterations: usize,

    /// Export name to connect to
    #[arg(short, long, default_value = "portalbd")]
    export: String,
}

// NBD protocol constants
const NBD_MAGIC: u64 = 0x4e42444d41474943;
const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054;
const NBD_REP_MAGIC: u64 = 0x3e889045565a9;
const NBD_OPT_GO: u32 = 7;
const NBD_REP_ACK: u32 = 1;
const NBD_REP_INFO: u32 = 3;
const NBD_INFO_EXPORT: u16 = 0;
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: u32 = 0x67446698;
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1;
const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1;
const NBD_FLAG_C_NO_ZEROES: u32 = 2;

struct NbdClient {
    stream: TcpStream,
    handle: u64,
    export_size: u64,
}

impl NbdClient {
    fn connect(addr: &str, export_name: &str) -> Result<Self> {
        let mut stream =
            TcpStream::connect(addr).with_context(|| format!("Failed to connect to {}", addr))?;

        // Read server greeting
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf)?;
        let magic = u64::from_be_bytes(buf);
        if magic != NBD_MAGIC {
            bail!("Invalid NBD magic: {:x}", magic);
        }

        stream.read_exact(&mut buf)?;
        let opts_magic = u64::from_be_bytes(buf);
        if opts_magic != NBD_OPTS_MAGIC {
            bail!("Invalid opts magic: {:x}", opts_magic);
        }

        let mut flags_buf = [0u8; 2];
        stream.read_exact(&mut flags_buf)?;
        let server_flags = u16::from_be_bytes(flags_buf);

        if server_flags & NBD_FLAG_FIXED_NEWSTYLE == 0 {
            bail!("Server doesn't support fixed newstyle");
        }

        // Send client flags
        let client_flags: u32 = NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES;
        stream.write_all(&client_flags.to_be_bytes())?;

        // Send NBD_OPT_GO to negotiate export
        let opt_magic = NBD_OPTS_MAGIC;
        stream.write_all(&opt_magic.to_be_bytes())?;
        stream.write_all(&NBD_OPT_GO.to_be_bytes())?;

        // Option data: export name length + name + no info requests
        let name_bytes = export_name.as_bytes();
        let opt_len = 4 + name_bytes.len() + 2; // name_len(4) + name + info_count(2)
        stream.write_all(&(opt_len as u32).to_be_bytes())?;
        stream.write_all(&(name_bytes.len() as u32).to_be_bytes())?;
        stream.write_all(name_bytes)?;
        stream.write_all(&0u16.to_be_bytes())?; // no info requests

        // Read reply(s)
        let mut export_size = 0u64;
        loop {
            // Reply magic
            let mut rep_buf = [0u8; 8];
            stream.read_exact(&mut rep_buf)?;
            let rep_magic = u64::from_be_bytes(rep_buf);
            if rep_magic != NBD_REP_MAGIC {
                bail!("Invalid reply magic: {:x}", rep_magic);
            }

            // Option and reply type
            let mut opt_buf = [0u8; 4];
            stream.read_exact(&mut opt_buf)?;
            let _opt = u32::from_be_bytes(opt_buf);

            stream.read_exact(&mut opt_buf)?;
            let rep_type = u32::from_be_bytes(opt_buf);

            // Reply length
            stream.read_exact(&mut opt_buf)?;
            let rep_len = u32::from_be_bytes(opt_buf) as usize;

            if rep_type == NBD_REP_ACK {
                // Done with negotiation
                break;
            } else if rep_type == NBD_REP_INFO {
                // Read info
                let mut info_data = vec![0u8; rep_len];
                stream.read_exact(&mut info_data)?;

                if rep_len >= 2 {
                    let info_type = u16::from_be_bytes([info_data[0], info_data[1]]);
                    if info_type == NBD_INFO_EXPORT && rep_len >= 12 {
                        export_size = u64::from_be_bytes([
                            info_data[2],
                            info_data[3],
                            info_data[4],
                            info_data[5],
                            info_data[6],
                            info_data[7],
                            info_data[8],
                            info_data[9],
                        ]);
                    }
                }
            } else if rep_type & 0x80000000 != 0 {
                // Error reply
                let mut err_data = vec![0u8; rep_len];
                stream.read_exact(&mut err_data)?;
                bail!("Server error: {:?}", String::from_utf8_lossy(&err_data));
            } else {
                // Unknown, skip
                let mut skip = vec![0u8; rep_len];
                stream.read_exact(&mut skip)?;
            }
        }

        Ok(Self {
            stream,
            handle: 0,
            export_size,
        })
    }

    fn read(&mut self, offset: u64, length: u32) -> Result<Vec<u8>> {
        self.handle += 1;
        let handle = self.handle;

        // Send request
        self.stream.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.stream.write_all(&0u16.to_be_bytes())?; // flags
        self.stream.write_all(&NBD_CMD_READ.to_be_bytes())?;
        self.stream.write_all(&handle.to_be_bytes())?;
        self.stream.write_all(&offset.to_be_bytes())?;
        self.stream.write_all(&length.to_be_bytes())?;

        // Read reply header
        let mut reply_buf = [0u8; 16];
        self.stream.read_exact(&mut reply_buf)?;

        let reply_magic =
            u32::from_be_bytes([reply_buf[0], reply_buf[1], reply_buf[2], reply_buf[3]]);
        if reply_magic != NBD_REPLY_MAGIC {
            bail!("Invalid reply magic");
        }

        let error = u32::from_be_bytes([reply_buf[4], reply_buf[5], reply_buf[6], reply_buf[7]]);
        if error != 0 {
            bail!("Read error: {}", error);
        }

        // Read data
        let mut data = vec![0u8; length as usize];
        self.stream.read_exact(&mut data)?;

        Ok(data)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        self.handle += 1;
        let handle = self.handle;

        // Send request
        self.stream.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.stream.write_all(&0u16.to_be_bytes())?; // flags
        self.stream.write_all(&NBD_CMD_WRITE.to_be_bytes())?;
        self.stream.write_all(&handle.to_be_bytes())?;
        self.stream.write_all(&offset.to_be_bytes())?;
        self.stream.write_all(&(data.len() as u32).to_be_bytes())?;
        self.stream.write_all(data)?;

        // Read reply
        let mut reply_buf = [0u8; 16];
        self.stream.read_exact(&mut reply_buf)?;

        let reply_magic =
            u32::from_be_bytes([reply_buf[0], reply_buf[1], reply_buf[2], reply_buf[3]]);
        if reply_magic != NBD_REPLY_MAGIC {
            bail!("Invalid reply magic");
        }

        let error = u32::from_be_bytes([reply_buf[4], reply_buf[5], reply_buf[6], reply_buf[7]]);
        if error != 0 {
            bail!("Write error: {}", error);
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.handle += 1;
        let handle = self.handle;

        // Send request
        self.stream.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.stream.write_all(&0u16.to_be_bytes())?; // flags
        self.stream.write_all(&NBD_CMD_FLUSH.to_be_bytes())?;
        self.stream.write_all(&handle.to_be_bytes())?;
        self.stream.write_all(&0u64.to_be_bytes())?; // offset (unused)
        self.stream.write_all(&0u32.to_be_bytes())?; // length (unused)

        // Read reply
        let mut reply_buf = [0u8; 16];
        self.stream.read_exact(&mut reply_buf)?;

        let reply_magic =
            u32::from_be_bytes([reply_buf[0], reply_buf[1], reply_buf[2], reply_buf[3]]);
        if reply_magic != NBD_REPLY_MAGIC {
            bail!("Invalid reply magic");
        }

        let error = u32::from_be_bytes([reply_buf[4], reply_buf[5], reply_buf[6], reply_buf[7]]);
        if error != 0 {
            bail!("Flush error: {}", error);
        }

        Ok(())
    }

    fn trim(&mut self, offset: u64, length: u32) -> Result<()> {
        self.handle += 1;
        let handle = self.handle;

        self.stream.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.stream.write_all(&0u16.to_be_bytes())?;
        self.stream.write_all(&NBD_CMD_TRIM.to_be_bytes())?;
        self.stream.write_all(&handle.to_be_bytes())?;
        self.stream.write_all(&offset.to_be_bytes())?;
        self.stream.write_all(&length.to_be_bytes())?;

        let mut reply_buf = [0u8; 16];
        self.stream.read_exact(&mut reply_buf)?;

        let error = u32::from_be_bytes([reply_buf[4], reply_buf[5], reply_buf[6], reply_buf[7]]);
        if error != 0 {
            bail!("Trim error: {}", error);
        }

        Ok(())
    }

    fn write_zeroes(&mut self, offset: u64, length: u32) -> Result<()> {
        self.handle += 1;
        let handle = self.handle;

        self.stream.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.stream.write_all(&0u16.to_be_bytes())?;
        self.stream.write_all(&NBD_CMD_WRITE_ZEROES.to_be_bytes())?;
        self.stream.write_all(&handle.to_be_bytes())?;
        self.stream.write_all(&offset.to_be_bytes())?;
        self.stream.write_all(&length.to_be_bytes())?;

        let mut reply_buf = [0u8; 16];
        self.stream.read_exact(&mut reply_buf)?;

        let error = u32::from_be_bytes([reply_buf[4], reply_buf[5], reply_buf[6], reply_buf[7]]);
        if error != 0 {
            bail!("WriteZeroes error: {}", error);
        }

        Ok(())
    }

    fn disconnect(&mut self) -> Result<()> {
        self.handle += 1;
        let handle = self.handle;

        self.stream.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.stream.write_all(&0u16.to_be_bytes())?;
        self.stream.write_all(&NBD_CMD_DISC.to_be_bytes())?;
        self.stream.write_all(&handle.to_be_bytes())?;
        self.stream.write_all(&0u64.to_be_bytes())?;
        self.stream.write_all(&0u32.to_be_bytes())?;

        Ok(())
    }
}

fn replay_trace(client: &mut NbdClient, trace: &OpTrace) -> Result<ReplayStats> {
    let start = Instant::now();
    let mut stats = ReplayStats::default();

    for traced in &trace.ops {
        let success = match &traced.op {
            NbdOp::Read { offset, length } => {
                stats.bytes_read += *length as u64;
                client.read(*offset, *length).is_ok()
            }
            NbdOp::Write { offset, length } => {
                // Generate deterministic data based on offset
                let data = vec![(*offset & 0xFF) as u8; *length as usize];
                stats.bytes_written += *length as u64;
                client.write(*offset, &data).is_ok()
            }
            NbdOp::Trim { offset, length } => client.trim(*offset, *length as u32).is_ok(),
            NbdOp::WriteZeroes { offset, length } => {
                stats.bytes_written += *length;
                client.write_zeroes(*offset, *length as u32).is_ok()
            }
            NbdOp::Flush => client.flush().is_ok(),
        };

        stats.ops_total += 1;
        if success {
            stats.ops_success += 1;
        } else {
            stats.ops_failed += 1;
        }
    }

    stats.duration_ns = start.elapsed().as_nanos() as u64;
    stats.compute_derived();

    Ok(stats)
}

#[derive(Debug, Default)]
struct ReplayStats {
    ops_total: u64,
    ops_success: u64,
    ops_failed: u64,
    duration_ns: u64,
    bytes_read: u64,
    bytes_written: u64,
    ops_per_sec: f64,
}

impl ReplayStats {
    fn compute_derived(&mut self) {
        let duration_secs = self.duration_ns as f64 / 1_000_000_000.0;
        if duration_secs > 0.0 {
            self.ops_per_sec = self.ops_total as f64 / duration_secs;
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("NBD Trace Replay Client");
    println!("=======================");

    let trace = load_trace(&args.trace)
        .with_context(|| format!("Failed to load trace: {}", args.trace.display()))?;

    println!("Trace: {}", trace.workload);
    println!("Operations: {}", trace.ops.len());
    println!("Server: {}", args.server);
    println!();

    for i in 0..args.iterations {
        println!("Iteration {}:", i + 1);

        let mut client = NbdClient::connect(&args.server, &args.export)
            .context("Failed to connect to NBD server")?;

        println!(
            "  Connected (export size: {} MB)",
            client.export_size / (1024 * 1024)
        );

        let stats = replay_trace(&mut client, &trace)?;

        println!(
            "  Duration: {:.2} ms",
            stats.duration_ns as f64 / 1_000_000.0
        );
        println!(
            "  Ops: {} ({:.0} ops/s)",
            stats.ops_total, stats.ops_per_sec
        );
        println!(
            "  Read: {} KB, Written: {} KB",
            stats.bytes_read / 1024,
            stats.bytes_written / 1024
        );

        if stats.ops_failed > 0 {
            println!("  Failed: {}", stats.ops_failed);
        }

        let _ = client.disconnect();
        println!();
    }

    Ok(())
}
