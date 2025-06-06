mod error;

use std::{io::Read, thread};

use binder::{binder_impl::IBinderInternal, check_service, SpIBinder};

/// The main entry of this crate
pub struct Dumpsys {
    service: SpIBinder,
}

impl Dumpsys {
    /// Retrieve an existing service and save it for dump, blocking for a few seconds if it doesn't yet exist.
    ///
    /// For example
    ///
    /// ```sh
    /// dumpsys SurfaceFlinger
    /// ```
    ///
    /// is equal to
    ///
    /// ```
    /// use dumpsys_rs::Dumpsys;
    ///
    /// Dumpsys::new("SurfaceFlinger");
    /// ```
    pub fn new<S>(service_name: S) -> Option<Self>
    where
        S: AsRef<str>,
    {
        let service = check_service(service_name.as_ref())?;
        Some(Self { service })
    }

    /// # Example
    ///
    /// ```
    /// use dumpsys_rs::Dumpsys;
    ///
    /// # fn foo() -> Option<()> {
    /// let result = Dumpsys::new("SurfaceFlinger")?
    ///     .dump(&["--latency"])
    ///     .unwrap();
    /// println!("{result}");
    /// # Some(())
    /// # }
    /// ```
    pub fn dump(&self, args: &'static [&str]) -> Result<String, error::DumpError> {
        let mut buf = String::new();

        {
            let mut service = self.service.clone();
            let (mut read, write) = os_pipe::pipe()?;
            let handle = thread::spawn(move || service.dump(&write, args));
            let _ = read.read_to_string(&mut buf);
            handle.join().unwrap()?;
        }

        Ok(buf)
    }

    /// # Example
    ///
    /// ```
    /// use dumpsys_rs::Dumpsys;
    ///
    /// # fn foo() -> Option<()> {
    /// let result = Dumpsys::new("SurfaceFlinger")?
    ///     .dump_to_byte::<1024>(&["--latency"])
    ///     .unwrap();
    /// println!("{result}");
    /// # Some(())
    /// # }
    /// ```
    pub fn dump_to_byte<const N: usize>(
        &self,
        args: &'static [&str],
    ) -> Result<[u8; N], error::DumpError> {
        let mut buf = [0u8; N];
        let mut total_read = 0;

        {
            let mut service = self.service.clone();
            let (mut read, write) = os_pipe::pipe()?;
            let handle = thread::spawn(move || service.dump(&write, args));
            while total_read < N {
                let n = read.read(&mut buf[total_read..])?;
                if n == 0 {
                    break;
                }
                total_read += n;
            }

            handle.join().unwrap()?;
        }
        Ok(buf)
    }
}
