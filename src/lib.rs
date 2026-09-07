pub mod error;
mod task_thread;

use std::{
    self,
    collections::HashMap,
    fs::OpenOptions,
    hash::BuildHasherDefault,
    io::{self, PipeReader, PipeWriter, Read as _},
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
};

use rsbinder::{ProcessState, SIBinder, StatusCode, hub};
use twox_hash::XxHash3_64;

use crate::{error::Error, task_thread::TaskThread};

type Result<T, E = crate::error::Error> = core::result::Result<T, E>;

/// One shot dumpsys
///
/// # Example
///
/// ```sh
/// dumpsys SurfaceFlinger
/// ```
///
/// is equal to
///
/// ```no_run
/// # fn foo() -> Result<(), dumpsys_rs::error::Error> {
/// dumpsys_rs::dump("SurfaceFlinger", &[])?;
/// # Ok(())
/// # }
/// ```
pub fn dump<S: AsRef<str>>(service_name: S, args: &[&str]) -> Result<String> {
    _ = ProcessState::init_default();

    let task_thread = TaskThread::spawn();

    let service = hub::get_service(service_name.as_ref()).ok_or(Error::ServiceNotExist)?;

    dump_inner(&task_thread, service, args)
}

/// Dumps a service into a fixed-size byte array, zero-padding short output and truncating long output.
///
/// # Example
///
/// ```no_run
/// # fn foo() -> Result<(), dumpsys_rs::error::Error> {
/// let bytes: [u8; 1024] = dumpsys_rs::dump_to_byte("SurfaceFlinger", &["--latency"])?;
/// # Ok(())
/// # }
/// ```
pub fn dump_to_byte<S: AsRef<str>, const N: usize>(
    service_name: S,
    args: &[&str],
) -> Result<[u8; N]> {
    _ = ProcessState::init_default();

    let task_thread = TaskThread::spawn();

    let service = hub::get_service(service_name.as_ref()).ok_or(Error::ServiceNotExist)?;

    dump_to_byte_inner(&task_thread, service, args)
}

/// Executes a dump request without reading or storing its output.
///
/// Output is discarded by the kernel through `/dev/null`; no pipe is created.
///
/// # Example
///
/// ```no_run
/// # fn foo() -> Result<(), dumpsys_rs::error::Error> {
/// dumpsys_rs::dump_only("SurfaceFlinger", &["--latency"])?;
/// # Ok(())
/// # }
/// ```
pub fn dump_only<S: AsRef<str>>(service_name: S, args: &[&str]) -> Result<()> {
    _ = ProcessState::init_default();

    let service = hub::get_service(service_name.as_ref()).ok_or(Error::ServiceNotExist)?;

    dump_only_inner(service, args)
}

#[repr(transparent)]
struct DumpArgs {
    inner: Box<[String]>,
}

impl FromIterator<String> for DumpArgs {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        let inner: Box<[String]> = iter.into_iter().collect();
        Self { inner }
    }
}

impl Deref for DumpArgs {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

type StatusI32Slot = Arc<AtomicI32>;

enum Task {
    Dump(DumpArgs, PipeWriter, SIBinder, StatusI32Slot),
    Shutdown,
}

/// Single retrieved existing services.
///
/// Like [`Dumpsys`], but use a task_thread exclusively.
pub struct BoundDumpsys {
    service: SIBinder,
    task_thread: TaskThread,
}

impl BoundDumpsys {
    /// Retrieve an existing service and save it for dump, blocking for a few seconds if it doesn't yet exist.
    ///
    /// # Example
    ///
    /// ```sh
    /// dumpsys SurfaceFlinger
    /// ```
    ///
    /// is equal to
    ///
    /// ```no_run
    /// use dumpsys_rs::BoundDumpsys;
    ///
    /// # fn foo() -> Result<(), dumpsys_rs::error::Error> {
    /// let mut dumpsys = BoundDumpsys::new("SurfaceFlinger")?;
    /// let result = dumpsys
    ///     .dump(&[])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<S: AsRef<str>>(service_name: S) -> Result<Self> {
        _ = ProcessState::init_default();

        Ok(Self {
            service: hub::get_service(service_name.as_ref()).ok_or(Error::ServiceNotExist)?,
            task_thread: TaskThread::spawn(),
        })
    }

    pub fn dump(&self, args: &[&str]) -> Result<String> {
        dump_inner(&self.task_thread, self.service.clone(), args)
    }

    /// Dumps the bound service into a fixed-size byte array, zero-padding short output and truncating long output.
    pub fn dump_to_byte<const N: usize>(&self, args: &[&str]) -> Result<[u8; N]> {
        dump_to_byte_inner(&self.task_thread, self.service.clone(), args)
    }

    /// Executes a dump request on the bound service without reading or storing its output.
    pub fn dump_only(&self, args: &[&str]) -> Result<()> {
        dump_only_inner(self.service.clone(), args)
    }
}

type XxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<XxHash3_64>>;

/// Retrieved existing services.
///
/// Drop [`Dumpsys`] will exit the background pipeing thread
pub struct Dumpsys {
    map: XxHashMap<Box<str>, SIBinder>,
    task_thread: TaskThread,
}

impl Dumpsys {
    pub fn new() -> Result<Self> {
        _ = ProcessState::init_default();

        Ok(Self {
            map: XxHashMap::default(),
            task_thread: TaskThread::spawn(),
        })
    }

    /// Retrieve an existing service and save it for dump, blocking for a few seconds if it doesn't yet exist.
    ///
    /// # Example
    ///
    /// ```sh
    /// dumpsys SurfaceFlinger
    /// ```
    ///
    /// is equal to
    ///
    /// ```no_run
    /// use dumpsys_rs::Dumpsys;
    ///
    /// # fn foo() -> Result<(), dumpsys_rs::error::Error> {
    /// let mut dumpsys = Dumpsys::new()?;
    /// dumpsys.insert_service("SurfaceFlinger")?;
    /// let result = dumpsys
    ///     .dump("SurfaceFlinger", &[])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_service<S: AsRef<str>>(&mut self, service_name: S) -> Result<bool> {
        let service_name = service_name.as_ref();

        let service = hub::get_service(service_name).ok_or(Error::ServiceNotExist)?;

        Ok(self.map.insert(Box::from(service_name), service).is_some())
    }

    /// Removes the selected service from the inner [`HashMap`].
    pub fn remove_service<S: AsRef<str>>(&mut self, service_name: S) -> Result<SIBinder> {
        let service_name = service_name.as_ref();

        self.map.remove(service_name).ok_or(Error::NoEntryFound)
    }

    pub fn dump<S: AsRef<str>>(&mut self, service_name: S, args: &[&str]) -> Result<String> {
        let service_name = service_name.as_ref();

        let service = self.map.get(service_name).ok_or(Error::NoEntryFound)?;

        dump_inner(&self.task_thread, service.clone(), args)
    }

    /// Dumps the selected service into a fixed-size byte array, zero-padding short output and truncating long output.
    pub fn dump_to_byte<S: AsRef<str>, const N: usize>(
        &mut self,
        service_name: S,
        args: &[&str],
    ) -> Result<[u8; N]> {
        let service_name = service_name.as_ref();

        let service = self.map.get(service_name).ok_or(Error::NoEntryFound)?;

        dump_to_byte_inner(&self.task_thread, service.clone(), args)
    }

    /// Executes a dump request on the selected service without reading or storing its output.
    pub fn dump_only<S: AsRef<str>>(&mut self, service_name: S, args: &[&str]) -> Result<()> {
        let service_name = service_name.as_ref();

        let service = self.map.get(service_name).ok_or(Error::NoEntryFound)?;

        dump_only_inner(service.clone(), args)
    }
}

fn dump_inner(task_thread: &TaskThread, service: SIBinder, args: &[&str]) -> Result<String> {
    let (mut reader, status_i32) = start_dump(task_thread, service, args)?;

    let mut buf = String::new();
    reader.read_to_string(&mut buf)?;
    check_status(&status_i32)?;

    Ok(buf)
}

fn dump_to_byte_inner<const N: usize>(
    task_thread: &TaskThread,
    service: SIBinder,
    args: &[&str],
) -> Result<[u8; N]> {
    let (mut reader, status_i32) = start_dump(task_thread, service, args)?;

    let buf = read_padded(&mut reader)?;
    check_status(&status_i32)?;

    Ok(buf)
}

fn dump_only_inner(service: SIBinder, args: &[&str]) -> Result<()> {
    let proxy = service.as_proxy().ok_or(Error::InvalidMethod)?;
    let null = OpenOptions::new().write(true).open("/dev/null")?;
    let dump_args = DumpArgs::from_iter(args.iter().copied().map(String::from));

    proxy.dump(null, &dump_args)?;

    Ok(())
}

fn start_dump(
    task_thread: &TaskThread,
    service: SIBinder,
    args: &[&str],
) -> Result<(PipeReader, StatusI32Slot)> {
    let (reader, writer) = io::pipe()?;

    let status_i32 = Arc::new(AtomicI32::new(i32::from(StatusCode::Ok)));

    task_thread
        .send(Task::Dump(
            DumpArgs::from_iter(args.iter().copied().map(String::from)),
            writer,
            service,
            status_i32.clone(),
        ))
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "task_thread dropped receiver"))?;

    Ok((reader, status_i32))
}

fn read_padded<const N: usize>(reader: &mut PipeReader) -> Result<[u8; N]> {
    let mut buf = [0u8; N];

    match reader.read_exact(&mut buf) {
        // Short output keeps the zero-padded tail.
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {}
        // Drain the rest so the task thread can finish and settle its status.
        Ok(()) => drain(reader)?,
        Err(err) => return Err(err.into()),
    }

    Ok(buf)
}

fn drain(reader: &mut PipeReader) -> Result<()> {
    let mut discard = [0u8; 8192];

    while reader.read(&mut discard)? != 0 {}

    Ok(())
}

fn check_status(status_i32: &StatusI32Slot) -> Result<()> {
    let status_code = StatusCode::from(status_i32.load(Ordering::Relaxed));

    if !matches!(status_code, StatusCode::Ok) {
        return Err(status_code.into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn short_output_is_zero_padded() {
        let (mut reader, mut writer) = io::pipe().unwrap();
        writer.write_all(b"abc").unwrap();
        drop(writer);

        assert_eq!(read_padded::<5>(&mut reader).unwrap(), *b"abc\0\0");
    }

    #[test]
    fn long_output_is_truncated() {
        let (mut reader, mut writer) = io::pipe().unwrap();
        let data: Vec<u8> = (0..40).collect();
        writer.write_all(&data).unwrap();
        drop(writer);

        assert_eq!(read_padded::<4>(&mut reader).unwrap(), [0, 1, 2, 3]);
    }
}
