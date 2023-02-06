import asyncio
import inspect
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from typing import Any, ContextManager, Dict


@asynccontextmanager
async def contextmanager_in_threadpool(
    *, cm: ContextManager, loop: asyncio.AbstractEventLoop
):
    try:
        yield await loop.run_in_executor(None, cm.__enter__)
    except Exception as e:
        try:
            await loop.run_in_executor(None, cm.__exit__, type(e), e, None)
        except:
            raise e
    else:
        # NOTE: if __exit__ is blocked due to waiting for an available thread, a race
        # condition is created/deadlock (depending on the function in the context
        # manager).
        #
        # See: https://github.com/tiangolo/fastapi/blob/5905c3f740c8590f1a370e36b99b760f1ee7b828/fastapi/concurrency.py#L20-L25
        await loop.run_in_executor(None, cm.__exit__, None, None, None)


async def resolve_dependencies(*, dependencies: Dict[str, Any], stack: AsyncExitStack):
    dependency_futures = {}
    loop = asyncio.get_running_loop()

    for name, dependency in dependencies.items():
        dependency_func = dependency.dependency

        if inspect.isasyncgenfunction(dependency_func):
            cm = asynccontextmanager(dependency_func)
            fut = stack.enter_async_context(cm())
        elif inspect.isgeneratorfunction(dependency_func):
            cm = contextmanager(dependency_func)
            fut = stack.enter_async_context(
                contextmanager_in_threadpool(cm=cm(), loop=loop)
            )
        elif inspect.iscoroutinefunction(dependency_func):
            fut = dependency_func()
        elif inspect.isfunction(dependency_func):
            fut = loop.run_in_executor(None, dependency_func)
        else:
            raise Exception("Dependency type not supported")

        dependency_futures[name] = fut

    dependency_results = await asyncio.gather(*dependency_futures.values())
    dependency_results_map = dict(zip(dependencies.keys(), dependency_results))

    return dependency_results_map
