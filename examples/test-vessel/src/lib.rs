use core_futures_io::copy;
use futures::task::SpawnExt;
use vessels::{export, VesselEntry};

export! {
    VesselEntry { spawner, mut reader, mut writer } => {
        spawner.spawn(async move {
            copy(&mut reader, &mut writer)
                .await
                .unwrap_or_else(|_| panic!());
        }).unwrap();
    }
}
