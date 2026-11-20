//! Dependencies: values the app provides, such as a database pool, that job functions take by
//! reference. Each is built lazily, once per process, the first time a job there asks for it.

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::OnceCell;

use crate::job::JobError;

type Shared = Arc<dyn Any + Send + Sync>;
type Factory = Arc<dyn Fn() -> BoxFuture<'static, Result<Shared, String>> + Send + Sync>;

/// A dependency a job takes: its type, as `check` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dependency {
    pub(crate) type_id: TypeId,
    pub(crate) type_name: &'static str,
}

impl Dependency {
    /// The dependency on a value of type `T`; used by the code `#[job]` generates.
    #[doc(hidden)]
    pub fn of<T: Any>() -> Self {
        Dependency { type_id: TypeId::of::<T>(), type_name: type_name::<T>() }
    }
}

/// How to build each dependency, and the ones built so far in this process.
#[derive(Default)]
pub struct Dependencies {
    factories: HashMap<TypeId, (Factory, &'static str)>,
    built: HashMap<TypeId, OnceCell<Shared>>,
}

impl std::fmt::Debug for Dependencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.factories.values().map(|(_, name)| *name).collect();
        f.debug_struct("Dependencies").field("provided", &names).finish_non_exhaustive()
    }
}

impl Dependencies {
    pub(crate) fn provide<T, F, Fut, E>(&mut self, factory: F)
    where
        T: Send + Sync + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: Display,
    {
        let factory: Factory = Arc::new(move || {
            let built = factory();
            Box::pin(async move {
                built.await.map(|value| Arc::new(value) as Shared).map_err(|error| error.to_string())
            })
        });
        let id = TypeId::of::<T>();
        self.factories.insert(id, (factory, type_name::<T>()));
        self.built.insert(id, OnceCell::new());
    }

    pub(crate) fn provides(&self, dependency: Dependency) -> bool {
        self.factories.contains_key(&dependency.type_id)
    }

    /// The value of type `T`, built on first use; used by the code `#[job]` generates.
    #[doc(hidden)]
    pub async fn get<T: Send + Sync + 'static>(&self) -> Result<Arc<T>, JobError> {
        let id = TypeId::of::<T>();
        let (Some((factory, _)), Some(cell)) = (self.factories.get(&id), self.built.get(&id)) else {
            return Err(JobError::msg(format!("the app does not provide {}", type_name::<T>())));
        };
        let shared = cell
            .get_or_try_init(|| factory())
            .await
            .map_err(|error| JobError::msg(format!("building {} failed: {error}", type_name::<T>())))?;
        Arc::clone(shared)
            .downcast::<T>()
            .map_err(|_| JobError::msg(format!("{} was provided with another type", type_name::<T>())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn builds_each_dependency_once_on_first_use() {
        static BUILDS: AtomicUsize = AtomicUsize::new(0);
        let mut dependencies = Dependencies::default();
        dependencies.provide(|| async {
            BUILDS.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::convert::Infallible>(42_u32)
        });
        assert_eq!(BUILDS.load(Ordering::SeqCst), 0);
        assert_eq!(*dependencies.get::<u32>().await.unwrap(), 42);
        assert_eq!(*dependencies.get::<u32>().await.unwrap(), 42);
        assert_eq!(BUILDS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn names_a_missing_dependency() {
        let dependencies = Dependencies::default();
        let error = dependencies.get::<String>().await.unwrap_err();
        assert!(format!("{error:?}").contains("does not provide"));
    }
}
