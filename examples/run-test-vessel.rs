use core::{
    convert::{Infallible, TryFrom},
    pin::Pin,
    task::{Context, Poll},
};
use core_futures_io::AsyncWrite;
use futures::executor::block_on;
use std::{fs::read, string::FromUtf8Error};
use vessels::{
    register,
    resource::ResourceManagerExt,
    runtime::{Runtime, Wasm},
    with_core, Convert, Core, MemoryStore, Ring, Sha256, SimpleResourceManager,
};
use vessels_wasmer::WasmerRuntime;

#[derive(Debug, Clone)]
pub struct Tester(String);

impl TryFrom<Vec<u8>> for Tester {
    type Error = FromUtf8Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        String::from_utf8(value).map(Tester)
    }
}

impl From<Tester> for Vec<u8> {
    fn from(tester: Tester) -> Vec<u8> {
        tester.0.as_bytes().into()
    }
}

pub struct TestWriter;

impl AsyncWrite for TestWriter {
    type WriteError = Infallible;
    type FlushError = Infallible;
    type CloseError = Infallible;

    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Self::WriteError>> {
        println!("got data {:?}", buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::FlushError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::CloseError>> {
        Poll::Ready(Ok(()))
    }
}

fn main() {
    let core = Core::new();

    with_core! { &core => { block_on(entry()) }};
}

async fn entry() {
    let mut manager = SimpleResourceManager::new();

    let mut store = MemoryStore::<Sha256>::new();

    manager.add_provider(store.clone()).await;

    register(move || {
        let manager = manager.clone();

        Box::pin(async move { Ok::<_, Infallible>(manager.into_erased()) })
    })
    .await
    .unwrap();

    let resource = store
        .intern::<Ring, _, Convert>(Wasm(
            read("target/wasm32-unknown-unknown/debug/test_vessel.wasm").unwrap(),
        ))
        .await
        .unwrap();

    let mut runtime = WasmerRuntime;

    runtime
        .instantiate(resource, TestWriter, [10u8, 2u8, 3u8, 50u8].as_ref())
        .await
        .unwrap();
}
