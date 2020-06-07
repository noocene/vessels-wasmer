use core_futures_io::{AsyncRead, AsyncWrite};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    task::{waker as make_waker, ArcWake, AtomicWaker},
    Future,
};
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use vessels::{
    acquire,
    resource::{ErasedResourceManager, ResourceManagerExt},
    runtime::{Runtime, RuntimeError, WasmResource},
};
use wasmer_runtime::{
    error::Error as WasmError, func, imports, instantiate, Func, Instance, Memory,
};

pub struct WasmerRuntime;

struct WasmerFuture<T: AsyncWrite, U: AsyncRead> {
    instance: Instance,
    waker: Arc<AtomicWaker>,
    reader_wakeup: Arc<AtomicBool>,
    writer_write_wakeup: Arc<AtomicBool>,
    writer_flush_wakeup: Arc<AtomicBool>,
    writer_close_wakeup: Arc<AtomicBool>,
    error: Receiver<RuntimeError<WasmError, U::Error, T::WriteError, T::FlushError, T::CloseError>>,
}

impl<T: AsyncWrite, U: AsyncRead> Future for WasmerFuture<T, U> {
    type Output =
        Result<(), RuntimeError<WasmError, U::Error, T::WriteError, T::FlushError, T::CloseError>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.waker.register(cx.waker());

        match self.error.try_next() {
            Ok(Some(e)) => return Poll::Ready(Err(e)),
            _ => {}
        }

        if self
            .reader_wakeup
            .compare_and_swap(true, false, Ordering::SeqCst)
        {
            self.instance
                .exports
                .get::<Func<(), ()>>("_vessel_wake_reader")
                .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?
                .call()
                .map_err(|e| RuntimeError::Runtime(WasmError::RuntimeError(e)))?;
        }

        if self
            .writer_write_wakeup
            .compare_and_swap(true, false, Ordering::SeqCst)
        {
            self.instance
                .exports
                .get::<Func<(), ()>>("_vessel_wake_writer_write")
                .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?
                .call()
                .map_err(|e| RuntimeError::Runtime(WasmError::RuntimeError(e)))?;
        }

        if self
            .writer_flush_wakeup
            .compare_and_swap(true, false, Ordering::SeqCst)
        {
            self.instance
                .exports
                .get::<Func<(), ()>>("_vessel_wake_writer_flush")
                .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?
                .call()
                .map_err(|e| RuntimeError::Runtime(WasmError::RuntimeError(e)))?;
        }

        if self
            .writer_close_wakeup
            .compare_and_swap(true, false, Ordering::SeqCst)
        {
            self.instance
                .exports
                .get::<Func<(), ()>>("_vessel_wake_writer_close")
                .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?
                .call()
                .map_err(|e| RuntimeError::Runtime(WasmError::RuntimeError(e)))?;
        }

        if self
            .instance
            .exports
            .get::<Func<(), i32>>("_vessel_poll")
            .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?
            .call()
            .map_err(|e| RuntimeError::Runtime(WasmError::RuntimeError(e)))?
            != 0
        {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

struct VesselWaker {
    wakeup: Arc<AtomicBool>,
    waker: Arc<AtomicWaker>,
}

impl ArcWake for VesselWaker {
    fn wake_by_ref(arc: &Arc<Self>) {
        arc.wakeup.store(true, Ordering::SeqCst);
        arc.waker.wake();
    }
}

struct VesselContext<T: AsyncWrite, U: AsyncRead> {
    reader: U,
    writer: T,
    waker: Arc<AtomicWaker>,
    error_sender: Option<
        Sender<RuntimeError<WasmError, U::Error, T::WriteError, T::FlushError, T::CloseError>>,
    >,
    memory: Option<Memory>,
}

impl<T: 'static + Send + Unpin + AsyncWrite, U: 'static + Send + Unpin + AsyncRead> Runtime<T, U>
    for WasmerRuntime
where
    RuntimeError<WasmError, U::Error, T::WriteError, T::FlushError, T::CloseError>: Send,
{
    type Instance = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (),
                        RuntimeError<
                            Self::Error,
                            U::Error,
                            T::WriteError,
                            T::FlushError,
                            T::CloseError,
                        >,
                    >,
                > + Send,
        >,
    >;
    type Error = WasmError;

    fn instantiate(&mut self, module: WasmResource, writer: T, reader: U) -> Self::Instance {
        Box::pin(async move {
            let (error_sender, error) = channel(0);

            let error_sender = Some(error_sender);

            let reader_wakeup = Arc::new(AtomicBool::new(false));
            let writer_write_wakeup = Arc::new(AtomicBool::new(false));
            let writer_flush_wakeup = Arc::new(AtomicBool::new(false));
            let writer_close_wakeup = Arc::new(AtomicBool::new(false));

            let manager: ErasedResourceManager =
                acquire().await?.ok_or(RuntimeError::NoResourceManager)?;

            let fetch = manager.fetch(module);
            let data = fetch.await?.ok_or(RuntimeError::NoBinary)?.0;

            let waker = Arc::new(AtomicWaker::new());

            let waker_handle = waker.clone();

            let reader_waker = make_waker(Arc::new(VesselWaker {
                wakeup: reader_wakeup.clone(),
                waker: waker.clone(),
            }));

            let writer_write_waker = make_waker(Arc::new(VesselWaker {
                wakeup: writer_write_wakeup.clone(),
                waker: waker.clone(),
            }));

            let writer_flush_waker = make_waker(Arc::new(VesselWaker {
                wakeup: writer_write_wakeup.clone(),
                waker: waker.clone(),
            }));

            let writer_close_waker = make_waker(Arc::new(VesselWaker {
                wakeup: writer_write_wakeup.clone(),
                waker: waker.clone(),
            }));

            let context = Arc::new(Mutex::new(VesselContext {
                reader,
                writer,
                waker: waker.clone(),
                memory: None,
                error_sender,
            }));

            let reader_context_handle = context.clone();
            let writer_write_context_handle = context.clone();
            let writer_flush_context_handle = context.clone();
            let writer_close_context_handle = context.clone();

            let instance = instantiate(
                &data,
                &imports! {
                    "env" => {
                        "_vessel_wake" => func!(move || {
                            waker_handle.wake();
                        }),
                        "_vessel_poll_read" => func!(move |ptr: i32, len: i32| -> i32 {
                            let context = &mut *(reader_context_handle.lock());

                            let mut buffer = vec![0u8; len as usize];

                            match Pin::new(&mut context.reader).poll_read(&mut Context::from_waker(&reader_waker), &mut buffer) {
                                Poll::Pending => 0,
                                Poll::Ready(Ok(len)) => {
                                    let view = context.memory.as_ref().unwrap().view::<u8>();

                                    buffer[..len].iter().zip(&view[ptr as usize..]).for_each(|(byte, cell)| {
                                        cell.set(*byte)
                                    });

                                    (len + 1) as i32
                                }
                                Poll::Ready(Err(e)) => {
                                    if let Some(mut sender) = context.error_sender.take() {
                                        let _ = sender.try_send(RuntimeError::Read(e));
                                    }
                                    context.waker.wake();

                                    0
                                }
                            }
                        }),
                        "_vessel_poll_write" => func!(move |ptr: i32, len: i32| -> i32 {
                            let (len, ptr) = (len as usize, ptr as usize);

                            let context = &mut *(writer_write_context_handle.lock());

                            let mut buffer = Vec::with_capacity(len);

                            let view = context.memory.as_ref().unwrap().view::<u8>();

                            for byte in &view[ptr..ptr + len] {
                                buffer.push(byte.get())
                            }

                            match Pin::new(&mut context.writer).poll_write(&mut Context::from_waker(&writer_write_waker), &buffer) {
                                Poll::Pending => 0,
                                Poll::Ready(Ok(len)) => {
                                    (len + 1) as i32
                                }
                                Poll::Ready(Err(e)) => {
                                    if let Some(mut sender) = context.error_sender.take() {
                                        let _ = sender.try_send(RuntimeError::Write(e));
                                    }
                                    context.waker.wake();

                                    0
                                }
                            }
                        }),
                        "_vessel_poll_flush" => func!(move || -> i32 {
                            let context = &mut *(writer_flush_context_handle.lock());

                            match Pin::new(&mut context.writer).poll_flush(&mut Context::from_waker(&writer_flush_waker)) {
                                Poll::Pending => 0,
                                Poll::Ready(Ok(())) => 1,
                                Poll::Ready(Err(e)) => {
                                    if let Some(mut sender) = context.error_sender.take() {
                                        let _ = sender.try_send(RuntimeError::Flush(e));
                                    }
                                    context.waker.wake();

                                    0
                                }
                            }
                        }),
                        "_vessel_poll_close" => func!(move || -> i32 {
                            let context = &mut *(writer_close_context_handle.lock());

                            match Pin::new(&mut context.writer).poll_close(&mut Context::from_waker(&writer_close_waker)) {
                                Poll::Pending => 0,
                                Poll::Ready(Ok(())) => 1,
                                Poll::Ready(Err(e)) => {
                                    if let Some(mut sender) = context.error_sender.take() {
                                        let _ = sender.try_send(RuntimeError::Close(e));
                                    }
                                    context.waker.wake();

                                    0
                                }
                            }
                        }),
                    }
                },
            )
            .map_err(RuntimeError::Runtime)?;

            context.lock().memory = Some(
                instance
                    .exports
                    .get::<Memory>("memory")
                    .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?,
            );

            instance
                .exports
                .get::<Func<(), ()>>("_vessel_entry")
                .map_err(|e| RuntimeError::Runtime(WasmError::ResolveError(e)))?
                .call()
                .map_err(|e| RuntimeError::Runtime(WasmError::RuntimeError(e)))?;

            WasmerFuture::<T, U> {
                instance,
                waker,
                reader_wakeup,
                writer_write_wakeup,
                writer_flush_wakeup,
                writer_close_wakeup,
                error,
            }
            .await
        })
    }
}
