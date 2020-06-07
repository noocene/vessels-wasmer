use core_futures_io::copy;
use vessels::{export, VesselEntry};
pub mod executor;
use executor::spawn;

export! {
    VesselEntry { mut reader, mut writer } => {
        spawn(async move {
            copy(&mut reader, &mut writer)
                .await
                .unwrap_or_else(|_| panic!());
        })
    }
}
