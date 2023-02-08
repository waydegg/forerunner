import asyncio
import inspect
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from typing import Any, Callable, ContextManager, Dict

from ipdb import set_trace


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
        # manager). The default executor (the running loop's ThreadPoolExecutor) which
        # *does* block if there are no available threads (I think).
        #
        # See: https://github.com/tiangolo/fastapi/blob/5905c3f740c8590f1a370e36b99b760f1ee7b828/fastapi/concurrency.py#L20-L25
        await loop.run_in_executor(None, cm.__exit__, None, None, None)


async def resolve_dependencies(*, dependencies: Dict[str, Any], stack: AsyncExitStack):
    async def resolve_dependency(dependency_func: Callable):
        loop = asyncio.get_running_loop()

        try:
            # Async Generator
            if inspect.isasyncgenfunction(dependency_func):
                cm = asynccontextmanager(dependency_func)
                coro = stack.enter_async_context(cm())
                return await coro
            # Sync Generator
            elif inspect.isgeneratorfunction(dependency_func):
                cm = contextmanager(dependency_func)
                coro = stack.enter_async_context(
                    contextmanager_in_threadpool(cm=cm(), loop=loop)
                )
                return await coro
            # Async Function
            elif inspect.iscoroutinefunction(dependency_func):
                coro = dependency_func()
                return await coro
            # Sync Function
            elif inspect.isfunction(dependency_func):
                return await loop.run_in_executor(None, dependency_func)
            else:
                raise Exception("Dependency type not supported")

        except asyncio.CancelledError as e:
            pass
        except Exception as e:
            raise e

    dependency_tasks = {}
    for name, dependency in dependencies.items():
        dependency_func = dependency.dependency
        dependency_task = asyncio.create_task(resolve_dependency(dependency_func))
        dependency_tasks[name] = dependency_task

    dependency_results = await asyncio.gather(*dependency_tasks.values())
    dependency_results_map = dict(zip(dependencies.keys(), dependency_results))

    return dependency_results_map
