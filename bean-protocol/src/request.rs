//! Handles parsing requests from clients.

use crate::{
    error::{Error, Result},
};
use derive_builder::Builder;
/// Export this so's people can get at the `ErrorKind` values and crap without having to import
/// nom into their project.
pub use nom;

use nom::{
    IResult,
    bytes::streaming::{take, take_while1},
    character::{is_alphanumeric, complete::{self as nomchar, char, crlf}},
    error::{Error as NomError, ErrorKind as NomErrorKind},
    sequence::{tuple, terminated},
};

/// Represents a beanstalkd request and its various arguments.
///
/// This has a lifetime that references the the byte slice it was parsed from, so their fates
/// are linked together.
#[derive(Debug, PartialEq)]
pub enum Request<'a> {
    /// Bury (or "fail" in the parlance of our times) a job by its id with a priority value.
    Bury(u64, u32),
    /// Delete a job by ID
    Delete(u64),
    /// Ignore a tube (aka "channel")
    Ignore(&'a str),
    /// Kick a number of buried/delayed jobs
    Kick(u64),
    /// Kick a specific job by id
    KickJob(u64),
    /// Get a list of all channels
    ListTubes,
    /// Get the currently [`Use`][Request::Use]d channel
    ListTubeUsed,
    /// Pause a channel for N seconds
    PauseTube(&'a str, u32),
    /// Look at a job by its id
    Peek(u64),
    /// Look at the next job in the failed list
    PeekBuried,
    /// Look at the next job in the delayed list
    PeekDelayed,
    /// Look at the next job in the ready list
    PeekReady,
    /// Put a job on the queue
    Put(u32, u32, u32, &'a [u8]),
    /// Close the current connection
    Quit,
    /// Release a job by id, with a priority and delay value attached
    Release(u64, u32, u32),
    /// Reserve the next job on this connection
    Reserve,
    /// Reserve a specific job by id
    ReserveJob(u64),
    /// Reserve the next job on this connection, not responding for N seconds until either a job
    /// becomes available or N seconds has passed
    ReserveWithTimeout(u64),
    /// Return stats about this server
    Stats,
    /// Return stats about a job
    StatsJob(u64),
    /// Return stats on a channel
    StatsTube(&'a str),
    /// Touch a job by id (reset its TTR value)
    Touch(u64),
    /// Use a channel (to put jobs into it AND to `peek` which is weird)
    Use(&'a str),
    /// Watch a channel (for job consumption)
    Watch(&'a str),
}

macro_rules! finish {
    ($op:expr) => {
        match $op {
            IResult::Ok(res) => Ok(res),
            IResult::Err(nom::Err::Error(e)) | IResult::Err(nom::Err::Failure(e)) => Err(Error::Parse(e)),
            IResult::Err(nom::Err::Incomplete(needed)) => Err(Error::ParseIncomplete(needed)),
        }
    }
}

fn is_alphanum_dash(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'-'
}

fn is_channel_nodash(chr: u8) -> bool {
    is_alphanumeric(chr) ||
        chr == b'+' ||
        chr == b'/' ||
        chr == b';' ||
        chr == b'.' ||
        chr == b'$' ||
        chr == b'_' ||
        chr == b'(' ||
        chr == b')'
}

fn is_channel(chr: u8) -> bool {
    is_alphanumeric(chr) ||
        is_channel_nodash(chr) ||
        chr == b'-'
}

fn parse_cmd<'a>(inp: &'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
    take_while1(is_alphanum_dash)(inp)
}

fn parse_put<'a>(inp: &'a [u8]) -> IResult<&'a [u8], (u32, u32, u32, u64)> {
    tuple((
        terminated(nomchar::u32, char(' ')),
        terminated(nomchar::u32, char(' ')),
        terminated(nomchar::u32, char(' ')),
        terminated(nomchar::u64, crlf),
    ))(inp)
}

fn parse_release<'a>(inp: &'a [u8]) -> IResult<&'a [u8], (u64, u32, u32)> {
    tuple((
        terminated(nomchar::u64, char(' ')),
        terminated(nomchar::u32, char(' ')),
        terminated(nomchar::u32, crlf),
    ))(inp)
}

fn parse_bury<'a>(inp: &'a [u8]) -> IResult<&'a [u8], (u64, u32)> {
    tuple((
        terminated(nomchar::u64, char(' ')),
        terminated(nomchar::u32, crlf),
    ))(inp)
}

fn parse_pause_tube<'a>(inp: &'a [u8]) -> Result<(&'a [u8], (&'a str, u32))> {
    let (rest, channel) = parse_channel(inp)?;
    let (rest, _) = finish!(parse_space(rest))?;
    let (rest, secs) = finish!(parse_u32_eol(rest))?;
    Ok((rest, (channel, secs)))
}

fn parse_data<'a>(inp: &'a [u8], num_bytes: usize) -> IResult<&'a [u8], &'a [u8]> {
    let mut parser = terminated(
        take(num_bytes),
        crlf,
    );
    parser(inp)
}

fn parse_space<'a>(inp: &'a [u8]) -> IResult<&'a [u8], ()> {
    char(' ')(inp)
        .map(|(rest, _)| (rest, ()))
}

fn parse_crlf<'a>(inp: &'a [u8]) -> IResult<&'a [u8], ()> {
    crlf(inp)
        .map(|(rest, _)| (rest, ()))
}

fn parse_channel_impl<'a>(inp: &'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
    take_while1(is_channel)(inp)
}

fn parse_channel<'a>(inp: &'a [u8]) -> Result<(&'a [u8], &'a str)> {
    let (rest, channel_ser) = finish!(parse_channel_impl(inp))?;
    // channels cannot start with dash
    if channel_ser[0] == b'-' {
        Err(Error::Parse(NomError { input: inp, code: NomErrorKind::Char }))?;
    }
    if channel_ser.len() > 200 {
        Err(Error::BadChannel)?;
    }
    Ok((rest, std::str::from_utf8(channel_ser)?))
}

fn parse_channel_eol<'a>(inp: &'a [u8]) -> Result<(&'a [u8], &'a str)> {
    let (rest, channel) = parse_channel(inp)?;
    let (rest, _) = finish!(parse_crlf(rest))?;
    Ok((rest, channel))
}

fn parse_u32_eol<'a>(inp: &'a [u8]) -> IResult<&'a [u8], u32> {
    terminated(nomchar::u32, crlf)(inp)
}

fn parse_u64_eol<'a>(inp: &'a [u8]) -> IResult<&'a [u8], u64> {
    terminated(nomchar::u64, crlf)(inp)
}

/// Configured some options our queue might have
#[derive(Builder, Debug, Default)]
#[builder(pattern = "owned")]
pub struct ParseOptions {
    /// How many bytes our job size can be
    #[builder(default = "u16::MAX as u64")]
    max_job_size: u64,
}

/// Takes a byte slice and parses it as a beanstalkd request (as per
/// https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt)
///
/// This is a zero-copy operation (except the integers, probably) that returns the remainder of
/// the byte slice after parsing is done, allowing for continued parsing.
pub fn parse_request<'a, 'b>(inp: &'a [u8], options: &'b ParseOptions) -> Result<'a, (&'a [u8], Request<'a>)> {
    let (rest, req) = finish!(parse_cmd(inp))?;
    let (rest, request) = match req {
        b"bury" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, (job_id, priority)) = finish!(parse_bury(rest))?;
            (rest, Request::Bury(job_id, priority))
        }
        b"delete" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, job_id) = finish!(parse_u64_eol(rest))?;
            (rest, Request::Delete(job_id))
        }
        b"ignore" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, channel) = parse_channel_eol(rest)?;
            (rest, Request::Ignore(channel))
        }
        b"kick" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, num) = finish!(parse_u64_eol(rest))?;
            (rest, Request::Kick(num))
        }
        b"kick-job" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, job_id) = finish!(parse_u64_eol(rest))?;
            (rest, Request::KickJob(job_id))
        }
        b"list-tubes" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::ListTubes)
        }
        b"list-tube-used" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::ListTubeUsed)
        }
        b"pause-tube" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, (channel, secs)) = parse_pause_tube(rest)?;
            (rest, Request::PauseTube(channel, secs))
        }
        b"peek" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, job_id) = finish!(parse_u64_eol(rest))?;
            (rest, Request::Peek(job_id))
        }
        b"peek-buried" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::PeekBuried)
        }
        b"peek-delayed" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::PeekDelayed)
        }
        b"peek-ready" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::PeekReady)
        }
        b"put" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, (priority, delay, ttr, num_bytes)) = finish!(parse_put(rest))?;
            if &num_bytes > &options.max_job_size {
                Err(Error::JobTooBig(num_bytes))?;
            }
            let (rest, data) = finish!(parse_data(rest, num_bytes as usize))?;
            (rest, Request::Put(priority, delay, ttr, data))
        }
        b"quit" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::Quit)
        }
        b"release" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, (job_id, priority, delay)) = finish!(parse_release(rest))?;
            (rest, Request::Release(job_id, priority, delay))
        }
        b"reserve" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::Reserve)
        }
        b"reserve-job" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, job_id) = finish!(parse_u64_eol(rest))?;
            (rest, Request::ReserveJob(job_id))
        }
        b"reserve-with-timeout" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, timeout) = finish!(parse_u64_eol(rest))?;
            (rest, Request::ReserveWithTimeout(timeout))
        }
        b"stats" => {
            let (rest, _) = finish!(parse_crlf(rest))?;
            (rest, Request::Stats)
        }
        b"stats-job" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, job_id) = finish!(parse_u64_eol(rest))?;
            (rest, Request::StatsJob(job_id))
        }
        b"stats-tube" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, channel) = parse_channel_eol(rest)?;
            (rest, Request::StatsTube(channel))
        }
        b"touch" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, job_id) = finish!(parse_u64_eol(rest))?;
            (rest, Request::Touch(job_id))
        }
        b"use" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, channel) = parse_channel_eol(rest)?;
            (rest, Request::Use(channel))
        }
        b"watch" => {
            let (rest, _) = finish!(parse_space(rest))?;
            let (rest, channel) = parse_channel_eol(rest)?;
            (rest, Request::Watch(channel))
        }
        _ => Err(Error::UnknownRequest(std::str::from_utf8(req)?))?,
    };
    Ok((rest, request))
}

#[cfg(test)]
mod tests {
    use super::*;
    use const_format::concatcp;
    use nom::error::ErrorKind;

    fn bad_channel_name() -> &'static str {
        "too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long-too-long"
    }

    fn parse_req<'a>(bytes: &'a [u8]) -> Result<'a, (&'a [u8], Request<'a>)> {
        let opts = ParseOptionsBuilder::default().build().unwrap();
        parse_request(bytes, &opts)
    }

    macro_rules! assert_incomplete {
        ($inp:expr) => {{
            let bytes = $inp;
            let res = parse_req(bytes).unwrap_err();
            match res {
                Error::ParseIncomplete(_needed) => {}
                _ => panic!("Expected Error::ParseIncomplete(..) but got {:?}", res),
            }
        }}
    }

    macro_rules! assert_err {
        ($inp:expr, $i:expr, $kind:expr) => {{
            let bytes = $inp;
            let res = parse_req(bytes).unwrap_err();
            match res {
                Error::Parse(nom) => assert_eq!((nom.input, nom.code), (&bytes[$i..], $kind)),
                _ => panic!("Expected Error::Parse(..) but got {:?}", res),
            }
        }}
    }

    macro_rules! test_cmd_bare {
        ($variant:ident, $name:expr, $misspell:expr) => {{
            {
                let (rest, cmd) = parse_req(concatcp!($name, "\r\n").as_bytes()).unwrap();
                assert_eq!(rest, &[]);
                assert_eq!(cmd, Request::$variant);
            }
            {
                let (rest, cmd) = parse_req(concatcp!($name, "\r\nget a job\r\n").as_bytes()).unwrap();
                assert_eq!(rest, b"get a job\r\n");
                assert_eq!(cmd, Request::$variant);
            }

            assert_eq!(parse_req(concatcp!($misspell, "\r\n").as_bytes()).unwrap_err(), Error::UnknownRequest($misspell));
            assert_err!(concatcp!($name, " 123\r\n").as_bytes(), $name.len(), ErrorKind::CrLf);
            assert_err!(concatcp!($name, " \r\n").as_bytes(), $name.len(), ErrorKind::CrLf);
            assert_err!(concatcp!($name, "\n").as_bytes(), $name.len(), ErrorKind::CrLf);
            assert_err!(concatcp!($name, "\r").as_bytes(), $name.len(), ErrorKind::CrLf);
        }}
    }

    macro_rules! test_cmd_u64 {
        ($variant:ident, $name:expr, $misspell:expr) => {{
            {
                let (rest, cmd) = parse_req(concatcp!($name, " 18446744073709551615\r\n").as_bytes()).unwrap();
                assert_eq!(rest, &[]);
                assert_eq!(cmd, Request::$variant(18_446_744_073_709_551_615));
            }
            {
                let (rest, cmd) = parse_req(concatcp!($name, " 0\r\nget a job\r\n").as_bytes()).unwrap();
                assert_eq!(rest, b"get a job\r\n");
                assert_eq!(cmd, Request::$variant(0));
            }

            assert_eq!(parse_req(concatcp!($misspell, " 123\r\n").as_bytes()).unwrap_err(), Error::UnknownRequest($misspell));
            assert_err!(concatcp!($name, " 18446744073709551616\r\n").as_bytes(), $name.len() + 1, ErrorKind::Digit);
            assert_err!(concatcp!($name, " asdfasdf\r\n").as_bytes(), $name.len() + 1, ErrorKind::Digit);
            assert_err!(concatcp!($name, " 1111 \r\n").as_bytes(), $name.len() + 5, ErrorKind::CrLf);
            assert_err!(concatcp!($name, " 1111\n").as_bytes(), $name.len() + 5, ErrorKind::CrLf);
            assert_err!(concatcp!($name, " 1111\r").as_bytes(), $name.len() + 5, ErrorKind::CrLf);
        }}
    }

    macro_rules! test_cmd_chan {
        ($variant:ident, $name:expr, $misspell:expr) => {{
            {
                let (rest, cmd) = parse_req(concatcp!($name, " intake.api.delicious_beans\r\n").as_bytes()).unwrap();
                assert_eq!(rest, &[]);
                assert_eq!(cmd, Request::$variant("intake.api.delicious_beans"));
            }
            {
                let (rest, cmd) = parse_req(concatcp!($name, " $fun-facts+about//sal;mon.and_a(brand-new).dance\r\nget a job\r\n").as_bytes()).unwrap();
                assert_eq!(rest, b"get a job\r\n");
                assert_eq!(cmd, Request::$variant("$fun-facts+about//sal;mon.and_a(brand-new).dance"));
            }
            {
                let req = format!("{} {}\r\n", $name, bad_channel_name());
                assert_eq!(parse_req(req.as_bytes()).unwrap_err(), Error::BadChannel);
            }

            assert_eq!(parse_req(concatcp!($misspell, " get-a-job\r\n").as_bytes()).unwrap_err(), Error::UnknownRequest($misspell));
            assert_err!(concatcp!($name, " -$fun-facts+about//sal;mon.and_a(brand-new).dance\r\nget a job\r\n").as_bytes(), $name.len() + 1, ErrorKind::Char);
            assert_err!(concatcp!($name, " \r\n").as_bytes(), $name.len() + 1, ErrorKind::TakeWhile1);
            assert_err!(concatcp!($name, " get-a-job \r\n").as_bytes(), $name.len() + 10, ErrorKind::CrLf);
            assert_err!(concatcp!($name, " get-a-job\n").as_bytes(), $name.len() + 10, ErrorKind::CrLf);
            assert_err!(concatcp!($name, " get-a-job\r").as_bytes(), $name.len() + 10, ErrorKind::CrLf);
        }}
    }

    #[test]
    fn bury() {
        {
            let (rest, cmd) = parse_req(b"bury 18446744073709551615 4294967295\r\n").unwrap();
            assert_eq!(rest, &[]);
            assert_eq!(cmd, Request::Bury(18_446_744_073_709_551_615, 4_294_967_295));
        }
        {
            let (rest, cmd) = parse_req(b"bury 0 0\r\ntesting\r\n").unwrap();
            assert_eq!(rest, b"testing\r\n");
            assert_eq!(cmd, Request::Bury(0, 0));
        }

        assert_eq!(parse_req(b"bruy 123 1231\r\n").unwrap_err(), Error::UnknownRequest("bruy"));
        assert_err!(b"bury 18446744073709551616 4294967295\r\n", 5, ErrorKind::Digit);
        assert_err!(b"bury 18446744073709551615 4294967296\r\n", 26, ErrorKind::Digit);
        assert_err!(b"bury asdfasdf\r\n", 5, ErrorKind::Digit);
        assert_err!(b"bury 1111\r\n", 9, ErrorKind::Char);
        assert_err!(b"bury 1111 \r\n", 10, ErrorKind::Digit);
        assert_err!(b"bury 1111 ddd\r\n", 10, ErrorKind::Digit);
        assert_err!(b"bury 1111 42424 \r\n", 15, ErrorKind::CrLf);
        assert_err!(b"bury 1111 42424\n", 15, ErrorKind::CrLf);
        assert_err!(b"bury 1111 42424\r", 15, ErrorKind::CrLf);
    }

    #[test]
    fn delete() {
        test_cmd_u64! { Delete, "delete", "dleete" }
    }

    #[test]
    fn ignore() {
        test_cmd_chan! { Ignore, "ignore", "ingore" }
    }

    #[test]
    fn kick() {
        test_cmd_u64! { Kick, "kick", "kcik" }
    }

    #[test]
    fn kick_job() {
        test_cmd_u64! { KickJob, "kick-job", "kick-jbo" }
    }

    #[test]
    fn list_tubes() {
        test_cmd_bare! { ListTubes, "list-tubes", "list-tuebs" }
    }

    #[test]
    fn list_tube_used() {
        test_cmd_bare! { ListTubeUsed, "list-tube-used", "list-tube-usde" }
    }

    #[test]
    fn pause_tube() {
        {
            let (rest, cmd) = parse_req(b"pause-tube $snacky-smores.123 4294967295\r\n").unwrap();
            assert_eq!(rest, &[]);
            assert_eq!(cmd, Request::PauseTube("$snacky-smores.123", 4_294_967_295));
        }
        {
            let (rest, cmd) = parse_req(b"pause-tube my-channel 5\r\ntesting\r\n").unwrap();
            assert_eq!(rest, b"testing\r\n");
            assert_eq!(cmd, Request::PauseTube("my-channel", 5));
        }
        {
            let req = format!("pause-tube {} 6969\r\n", bad_channel_name());
            assert_eq!(parse_req(req.as_bytes()).unwrap_err(), Error::BadChannel);
        }

        assert_eq!(parse_req(b"pause-teub frisky 1231\r\n").unwrap_err(), Error::UnknownRequest("pause-teub"));
        assert_err!(b"pause-tube -galaxia 4294967295\r\n", 11, ErrorKind::Char);
        assert_err!(b"pause-tube galaxia 4294967296\r\n", 19, ErrorKind::Digit);
        assert_err!(b"pause-tube galaxia\r\n", 18, ErrorKind::Char);
        assert_err!(b"pause-tube galaxia \r\n", 19, ErrorKind::Digit);
        assert_err!(b"pause-tube galaxia ddd\r\n", 19, ErrorKind::Digit);
        assert_err!(b"pause-tube galaxia 42424 \r\n", 24, ErrorKind::CrLf);
        assert_err!(b"pause-tube galaxia 42424\n", 24, ErrorKind::CrLf);
        assert_err!(b"pause-tube galaxia 42424\r", 24, ErrorKind::CrLf);
    }

    #[test]
    fn peek() {
        test_cmd_u64! { Peek, "peek", "peak" }
    }

    #[test]
    fn peek_buried() {
        test_cmd_bare! { PeekBuried, "peek-buried", "peek-beried" }
    }

    #[test]
    fn peek_delayed() {
        test_cmd_bare! { PeekDelayed, "peek-delayed", "peak-delayed" }
    }

    #[test]
    fn peek_ready() {
        test_cmd_bare! { PeekReady, "peek-ready", "peak-reddy" }
    }

    #[test]
    fn put() {
        {
            let (rest, cmd) = parse_req(b"put 4294967295 4294967295 4294967295 9\r\nget a job\r\n").unwrap();
            assert_eq!(rest, &[]);
            assert_eq!(cmd, Request::Put(4294967295, 4294967295, 4294967295, b"get a job"));
        }
        {
            let (rest, cmd) = parse_req(b"put 4294967295 4294967295 4294967295 9\r\nget a job\r\ntesting\r\n").unwrap();
            assert_eq!(rest, b"testing\r\n");
            assert_eq!(cmd, Request::Put(4294967295, 4294967295, 4294967295, b"get a job"));
        }

        assert_eq!(parse_req(b"putt 1024 0 300 9\nget a job\r\n").unwrap_err(), Error::UnknownRequest("putt"));
        assert_incomplete!(b"put");
        assert_err!(b"put ", 4, ErrorKind::Digit);
        assert_err!(b"put 1", 5, ErrorKind::Char);
        assert_err!(b"put 1 ", 6, ErrorKind::Digit);
        assert_err!(b"put 1 1", 7, ErrorKind::Char);
        assert_err!(b"put 1 1 ", 8, ErrorKind::Digit);
        assert_err!(b"put 1 1 1", 9, ErrorKind::Char);
        assert_err!(b"put 1 1 1 ", 10, ErrorKind::Digit);
        assert_err!(b"put 1 1 1 5", 11, ErrorKind::CrLf);
        assert_incomplete!(b"put 1 1 1 5\r\n");
        assert_incomplete!(b"put 1 1 1 5\r\nasdf");
        assert_err!(b"put 1 1 1 5\r\nasdfc", 18, ErrorKind::CrLf);
        assert_err!(b"put 4294967296 123 123 9\r\nget a job\r\n", 4, ErrorKind::Digit);
        assert_err!(b"put 123 4294967296 123 9\r\nget a job\r\n", 8, ErrorKind::Digit);
        assert_err!(b"put 123 123 4294967296 9\r\nget a job\r\n", 12, ErrorKind::Digit);
        assert_err!(b"put 123 123 123 18446744073709551616\r\nget a job\r\n", 16, ErrorKind::Digit);
        assert_err!(b"put a123 123 123 9\r\nget a job\r\n", 4, ErrorKind::Digit);
        assert_err!(b"put 123 a123 123 9\r\nget a job\r\n", 8, ErrorKind::Digit);
        assert_err!(b"put 123 123 a123 9\r\nget a job\r\n", 12, ErrorKind::Digit);
        assert_err!(b"put 123 123 123 z9\r\nget a job\r\n", 16, ErrorKind::Digit);
        assert_err!(b"put 123 123 123 9\r\npoo get a job\r\n", 28, ErrorKind::CrLf);
        assert_incomplete!(b"put 123 123 123 9\r\na job\r\n");
        assert!(matches!(parse_req(b"put 1024 0 300 80000\r\nget a job\r\n").unwrap_err(), Error::JobTooBig(_)));
        parse_request(b"put 1024 0 300 9\r\nget a job\r\n", &ParseOptionsBuilder::default().max_job_size(9).build().unwrap()).unwrap();
        assert!(matches!(parse_request(b"put 1024 0 300 10\r\nget a job!\r\n", &ParseOptionsBuilder::default().max_job_size(9).build().unwrap()).unwrap_err(), Error::JobTooBig(_)));
    }

    #[test]
    fn quit() {
        test_cmd_bare! { Quit, "quit", "quiet" }
    }

    #[test]
    fn release() {
        {
            let (rest, cmd) = parse_req(b"release 18446744073709551615 4294967295 4294967295\r\n").unwrap();
            assert_eq!(rest, &[]);
            assert_eq!(cmd, Request::Release(18_446_744_073_709_551_615, 4_294_967_295, 4_294_967_295));
        }
        {
            let (rest, cmd) = parse_req(b"release 123 456 0011222\r\ntesting\r\n").unwrap();
            assert_eq!(rest, b"testing\r\n");
            assert_eq!(cmd, Request::Release(123, 456, 11222));
        }

        assert_eq!(parse_req(b"rewease 222 3333 4444\r\n").unwrap_err(), Error::UnknownRequest("rewease"));
        assert_incomplete!(b"release");
        assert_err!(b"release ", 8, ErrorKind::Digit);
        assert_err!(b"release zex", 8, ErrorKind::Digit);
        assert_err!(b"release 18446744073709551616 123 123\r\n", 8, ErrorKind::Digit);
        assert_err!(b"release 18446744073709551615 4294967296 123\r\n", 29, ErrorKind::Digit);
        assert_err!(b"release 18446744073709551615 4294967295 4294967296\r\n", 40, ErrorKind::Digit);
        assert_err!(b"release 111", 11, ErrorKind::Char);
        assert_err!(b"release 111 ", 12, ErrorKind::Digit);
        assert_err!(b"release 111 aa", 12, ErrorKind::Digit);
        assert_err!(b"release 111 42", 14, ErrorKind::Char);
        assert_err!(b"release 111 42 ", 15, ErrorKind::Digit);
        assert_err!(b"release 111 42 6969", 19, ErrorKind::CrLf);
        assert_err!(b"release 111 42 6969 ", 19, ErrorKind::CrLf);
        assert_err!(b"release 111 42 6969\r", 19, ErrorKind::CrLf);
        assert_err!(b"release 111 42 6969\n", 19, ErrorKind::CrLf);
    }

    #[test]
    fn reserve() {
        test_cmd_bare! { Reserve, "reserve", "resreve" }
    }

    #[test]
    fn reserve_job() {
        test_cmd_u64! { ReserveJob, "reserve-job", "resevre-job" }
    }

    #[test]
    fn reserve_with_timeout() {
        test_cmd_u64! { ReserveWithTimeout, "reserve-with-timeout", "reesrve-with-timeout" }
    }

    #[test]
    fn stats() {
        test_cmd_bare! { Stats, "stats", "sttas" }
    }

    #[test]
    fn stats_job() {
        test_cmd_u64! { StatsJob, "stats-job", "statz-job" }
    }

    #[test]
    fn stats_tube() {
        test_cmd_chan! { StatsTube, "stats-tube", "stats-tuube" }
    }

    #[test]
    fn touch() {
        test_cmd_u64! { Touch, "touch", "tuoch" }
    }

    #[test]
    fn useee() {
        test_cmd_chan! { Use, "use", "ues" }
    }

    #[test]
    fn watch() {
        test_cmd_chan! { Watch, "watch", "wach" }
    }

    #[test]
    fn parse_multi() {
        let multi = b"use barky\r\nput 1024 0 300 15\r\nbark bark bark!\r\nwatch barky\r\nwatch dogs\r\nreserve\r\ndelete 1231231\r\nquit\r\n";
        let (rest, cmd1) = parse_req(multi).unwrap();
        assert_eq!(cmd1, Request::Use("barky"));
        let (rest, cmd2) = parse_req(rest).unwrap();
        assert_eq!(cmd2, Request::Put(1024, 0, 300, b"bark bark bark!"));
        let (rest, cmd3) = parse_req(rest).unwrap();
        assert_eq!(cmd3, Request::Watch("barky"));
        let (rest, cmd4) = parse_req(rest).unwrap();
        assert_eq!(cmd4, Request::Watch("dogs"));
        let (rest, cmd5) = parse_req(rest).unwrap();
        assert_eq!(cmd5, Request::Reserve);
        let (rest, cmd6) = parse_req(rest).unwrap();
        assert_eq!(cmd6, Request::Delete(1231231));
        let (rest, cmd7) = parse_req(rest).unwrap();
        assert_eq!(cmd7, Request::Quit);
        assert_eq!(rest, b"");
    }

    #[test]
    fn eof() {
        let res = parse_req(b"use bar");
        assert!(matches!(res.unwrap_err(), Error::ParseIncomplete(_)));
    }
}

