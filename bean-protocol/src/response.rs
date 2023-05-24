//! Handles creating responses to clients.

use crate::{
    error::{Result},
};
use std::io::Write;

/// Represents a response to a client.
pub enum Response<'a> {
    /// You done messed up A-A-ron.
    BadFormat,
    /// A Job was buried while performing an operation on an existing job
    Buried,
    /// A job was buried while performing an operation on a new job
    BuriedJobID(u64),
    /// A job's TTR is about to expire *gasp*
    DeadlineSoon,
    /// A job was deleted, good job
    Deleted,
    /// The server is draining and will not acept new jobs
    Draining,
    /// A CRLF was expected but not encountered
    ExpectedCRLF,
    /// When a peek command returns a job
    Found(u64, &'a [u8]),
    /// A job was inserted
    Inserted(u64),
    /// An internal error (a bug)
    InternalError,
    /// The given job is too big
    JobTooBig,
    /// A single job was kicked
    Kicked,
    /// Many jobs were kicked
    KickedJobs(u64),
    /// The given job wasn't found
    NotFound,
    /// Sent when an ignore is issued against the last channel in the watch list
    NotIgnored,
    /// A YAML response, probably to a stats or list command
    Ok(&'a str),
    /// Indicates the server is out of memory and can't possibly accept one more job
    OutOfMemory,
    /// A channel was paused
    Paused,
    /// A job was released, wow
    Released,
    /// A job was reserved, here it is
    Reserved(u64, &'a [u8]),
    /// A reserve-with-timeout timed out
    TimedOut,
    /// A job's TTR was reset
    Touched,
    /// Unknown command
    UnknownCommand,
    /// Successfully `use`ed a channel
    Using(&'a str),
    /// A channel was watched
    Watching(u32),
}

impl<'a> Response<'a> {
    pub fn serialize_into<W>(&'a self, mut writer: W) -> Result<usize>
        where W: Write,
    {
        let res = match self {
            Self::BadFormat => writer.write(b"BAD_FORMAT\r\n")?,
            Self::Buried => writer.write(b"BURIED\r\n")?,
            Self::BuriedJobID(job_id) => {
                let n1 = writer.write(b"BURIED ")?;
                let n2 = writer.write(job_id.to_string().as_bytes())?;
                let n3 = writer.write(b"\r\n")?;
                n1 + n2 + n3
            }
            Self::DeadlineSoon => writer.write(b"DEADLINE_SOON\r\n")?,
            Self::Deleted => writer.write(b"DELETED\r\n")?,
            Self::Draining => writer.write(b"DRAINING\r\n")?,
            Self::ExpectedCRLF => writer.write(b"EXPECTED_CRLF\r\n")?,
            Self::Found(job_id, job_data) => {
                let n1 = writer.write(b"FOUND ")?;
                let n2 = writer.write(job_id.to_string().as_bytes())?;
                let n3 = writer.write(b" ")?;
                let n4 = writer.write(job_data.len().to_string().as_bytes())?;
                let n5 = writer.write(b"\r\n")?;
                let n6 = writer.write(job_data)?;
                let n7 = writer.write(b"\r\n")?;
                n1 + n2 + n3 + n4 + n5 + n6 + n7
            }
            Self::Inserted(job_id) => {
                let n1 = writer.write(b"INSERTED ")?;
                let n2 = writer.write(job_id.to_string().as_bytes())?;
                let n3 = writer.write(b"\r\n")?;
                n1 + n2 + n3
            }
            Self::InternalError => writer.write(b"INTERNAL_ERROR\r\n")?,
            Self::JobTooBig => writer.write(b"JOB_TOO_BIG\r\n")?,
            Self::Kicked => writer.write(b"KICKED\r\n")?,
            Self::KickedJobs(num) => {
                let n1 = writer.write(b"KICKED ")?;
                let n2 = writer.write(num.to_string().as_bytes())?;
                let n3 = writer.write(b"\r\n")?;
                n1 + n2 + n3
            }
            Self::NotFound => writer.write(b"NOT_FOUND\r\n")?,
            Self::NotIgnored => writer.write(b"NOT_IGNORED\r\n")?,
            Self::Ok(yaml) => {
                let n1 = writer.write(b"OK ")?;
                let n2 = writer.write(yaml.as_bytes().len().to_string().as_bytes())?;
                let n3 = writer.write(b"\r\n")?;
                let n4 = writer.write(yaml.as_bytes())?;
                let n5 = writer.write(b"\r\n")?;
                n1 + n2 + n3 + n4 + n5
            }
            Self::OutOfMemory => writer.write(b"OUT_OF_MEMORY\r\n")?,
            Self::Paused => writer.write(b"PAUSED\r\n")?,
            Self::Released => writer.write(b"RELEASED\r\n")?,
            Self::Reserved(job_id, job_data) => {
                let n1 = writer.write(b"RESERVED ")?;
                let n2 = writer.write(job_id.to_string().as_bytes())?;
                let n3 = writer.write(b" ")?;
                let n4 = writer.write(job_data.len().to_string().as_bytes())?;
                let n5 = writer.write(b"\r\n")?;
                let n6 = writer.write(job_data)?;
                let n7 = writer.write(b"\r\n")?;
                n1 + n2 + n3 + n4 + n5 + n6 + n7
            }
            Self::TimedOut => writer.write(b"TIMED_OUT\r\n")?,
            Self::Touched => writer.write(b"TOUCHED\r\n")?,
            Self::UnknownCommand => writer.write(b"UNKNOWN_COMMAND\r\n")?,
            Self::Using(chan) => {
                let n1 = writer.write(b"USING ")?;
                let n2 = writer.write(chan.as_bytes())?;
                let n3 = writer.write(b"\r\n")?;
                n1 + n2 + n3
            }
            Self::Watching(num) => {
                let n1 = writer.write(b"WATCHING ")?;
                let n2 = writer.write(num.to_string().as_bytes())?;
                let n3 = writer.write(b"\r\n")?;
                n1 + n2 + n3
            }
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_bare {
        ($variant:expr, $expect:expr) => {{
            let mut buf = [0u8; 1024];
            let expect: &[u8] = $expect;
            let res = $variant;
            let n = res.serialize_into(buf.as_mut_slice()).unwrap();
            assert_eq!(n, expect.len());
            assert_eq!(&buf[0..n], expect);
        }}
    }

    #[test]
    fn bad_format() {
        test_bare! { Response::BadFormat, b"BAD_FORMAT\r\n" }
    }

    #[test]
    fn buried() {
        test_bare! { Response::Buried, b"BURIED\r\n" }
    }

    #[test]
    fn buried_job_id() {
        test_bare! { Response::BuriedJobID(299212), b"BURIED 299212\r\n" }
        test_bare! { Response::BuriedJobID(0), b"BURIED 0\r\n" }
        test_bare! { Response::BuriedJobID(829817231298), b"BURIED 829817231298\r\n" }
    }

    #[test]
    fn deadline_soon() {
        test_bare! { Response::DeadlineSoon, b"DEADLINE_SOON\r\n" }
    }

    #[test]
    fn deleted() {
        test_bare! { Response::Deleted, b"DELETED\r\n" }
    }

    #[test]
    fn draining() {
        test_bare! { Response::Draining, b"DRAINING\r\n" }
    }

    #[test]
    fn expected_crlf() {
        test_bare! { Response::ExpectedCRLF, b"EXPECTED_CRLF\r\n" }
    }

    #[test]
    fn found() {
        test_bare! {
            Response::Found(0, &[32; 128]),
            b"FOUND 0 128\r\n                                                                                                                                \r\n"
        }
        test_bare! {
            Response::Found(1298127, b"i hope i get a jobby, freddy"),
            b"FOUND 1298127 28\r\ni hope i get a jobby, freddy\r\n"
        }
        test_bare! {
            Response::Found(999999999910101010, b"Sixteen days??? How did you hold your bref for dat long??"),
            b"FOUND 999999999910101010 57\r\nSixteen days??? How did you hold your bref for dat long??\r\n"
        }
    }

    #[test]
    fn inthert() {
        test_bare! { Response::Inserted(24), b"INSERTED 24\r\n" }
        test_bare! { Response::Inserted(9182731908273), b"INSERTED 9182731908273\r\n" }
        test_bare! { Response::Inserted(0), b"INSERTED 0\r\n" }
    }

    #[test]
    fn internal_error() {
        test_bare! { Response::InternalError, b"INTERNAL_ERROR\r\n" }
    }

    #[test]
    fn job_too_big() {
        test_bare! { Response::JobTooBig, b"JOB_TOO_BIG\r\n" }
    }

    #[test]
    fn kicked() {
        test_bare! { Response::Kicked, b"KICKED\r\n" }
    }

    #[test]
    fn kicked_jobs() { 
        test_bare! { Response::KickedJobs(42), b"KICKED 42\r\n" }
        test_bare! { Response::KickedJobs(98817203489712), b"KICKED 98817203489712\r\n" }
        test_bare! { Response::KickedJobs(0), b"KICKED 0\r\n" }
    }

    #[test]
    fn not_found() {
        test_bare! { Response::NotFound, b"NOT_FOUND\r\n" }
    }

    #[test]
    fn not_ignored() {
        test_bare! { Response::NotIgnored, b"NOT_IGNORED\r\n" }
    }

    #[test]
    fn ok() {
        test_bare! {
            Response::Ok("---\r\nname: frank\r\nhas: donkey brains"),
            b"OK 36\r\n---\r\nname: frank\r\nhas: donkey brains\r\n"
        }
        test_bare! {
            Response::Ok("---\r\nname: butch\r\ncanine: true\r\neats: cat poop"),
            b"OK 46\r\n---\r\nname: butch\r\ncanine: true\r\neats: cat poop\r\n"
        }
    }

    #[test]
    fn out_of_memory() {
        test_bare! { Response::OutOfMemory, b"OUT_OF_MEMORY\r\n" }
    }

    #[test]
    fn paused() {
        test_bare! { Response::Paused, b"PAUSED\r\n" }
    }

    #[test]
    fn released() {
        test_bare! { Response::Released, b"RELEASED\r\n" }
    }

    #[test]
    fn reserved() {
        test_bare! {
            Response::Reserved(999101010, b"hi i'm butch"),
            b"RESERVED 999101010 12\r\nhi i'm butch\r\n"
        }
        test_bare! {
            Response::Reserved(0, b"get a job\r\nget a job\r\n"),
            b"RESERVED 0 22\r\nget a job\r\nget a job\r\n\r\n"
        }
    }

    #[test]
    fn timed_out() {
        test_bare! { Response::TimedOut, b"TIMED_OUT\r\n" }
    }

    #[test]
    fn touched() {
        test_bare! { Response::Touched, b"TOUCHED\r\n" }
    }

    #[test]
    fn unknown_command() {
        test_bare! { Response::UnknownCommand, b"UNKNOWN_COMMAND\r\n" }
    }

    #[test]
    fn using() {
        test_bare! {
            Response::Using("snacky"),
            b"USING snacky\r\n"
        }
        test_bare! {
            Response::Using("$intake.api-liberal-snowflake"),
            b"USING $intake.api-liberal-snowflake\r\n"
        }
    }

    #[test]
    fn watching() {
        test_bare! { Response::Watching(42), b"WATCHING 42\r\n" }
        test_bare! { Response::Watching(0), b"WATCHING 0\r\n" }
        test_bare! { Response::Watching(999991010), b"WATCHING 999991010\r\n" }
    }
}

