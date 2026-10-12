"""
Minimal stand-ins for FastAPI and pydantic, so app/api.py can be IMPORTED and its endpoint functions CALLED DIRECTLY where the real packages are not installed
(as in the environment this project is developed in). They do just enough: a decorator that returns the function, a registry of handlers and middleware,
models that take keyword arguments, apply defaults and run field validators. They are not a web server: the real thing is exercised by validate_api.py.

install() does nothing when the real packages import, and then the same tests call the same functions through the real framework's decorators.
"""
import sys
import types


NAMES = ("fastapi", "fastapi.responses", "fastapi.staticfiles", "pydantic", "app.api")


def snapshot() -> dict:
    """What those modules are right now, to put back afterwards: a test that installs the stand-ins must not leave them for the next one."""
    return {k: sys.modules.get(k) for k in NAMES}


def restore(saved: dict) -> None:
    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


def install(force: bool = False) -> bool:
    """Returns True if the stand-ins were installed, False if the real packages are present (and force is not set). Also forgets any imported app.api, so the next
    import of it is built on whatever is installed NOW."""
    if not force:
        try:
            import fastapi  # noqa: F401
            import pydantic  # noqa: F401
            return False
        except ImportError:
            pass
    sys.modules.pop("app.api", None)

    fastapi = types.ModuleType("fastapi")
    responses = types.ModuleType("fastapi.responses")
    staticfiles = types.ModuleType("fastapi.staticfiles")
    pydantic = types.ModuleType("pydantic")

    class HTTPException(Exception):
        def __init__(self, status_code, detail=None, headers=None):
            super().__init__(detail)
            self.status_code, self.detail, self.headers = status_code, detail, headers or {}

    class _Response:
        def __init__(self, content=None, status_code=200, **kw):
            self.content, self.status_code = content, status_code

    class JSONResponse(_Response):
        pass

    class FileResponse(_Response):
        def __init__(self, path, **kw):
            super().__init__(path)

    class StaticFiles:
        def __init__(self, **kw):
            pass

    class FastAPI:
        def __init__(self, **kw):
            self.exception_handlers, self.middlewares, self.routes = {}, [], {}

        def _route(self, method):
            def factory(path, **kw):
                def register(fn):
                    self.routes[(method, path)] = fn
                    return fn
                return register
            return factory

        def __getattr__(self, name):
            if name in ("get", "post", "put", "delete", "patch"):
                return self._route(name.upper())
            raise AttributeError(name)

        def exception_handler(self, exc_class):
            def register(fn):
                self.exception_handlers[exc_class] = fn
                return fn
            return register

        def middleware(self, kind):
            def register(fn):
                self.middlewares.append((kind, fn))
                return fn
            return register

        def mount(self, *a, **kw):
            pass

    class _Marker:
        def __init__(self, dependency=None):
            self.dependency = dependency

    def Depends(dependency=None):
        return _Marker(dependency)

    def Header(default=None, **kw):
        return default

    class ValidationError(ValueError):
        pass

    class _FieldInfo:
        def __init__(self, default, kw):
            self.default, self.kw = default, kw

    def Field(default=..., **kw):
        return _FieldInfo(default, kw)

    def field_validator(*names):
        def decorate(obj):
            (obj.__func__ if isinstance(obj, classmethod) else obj).__validates__ = names
            return obj
        return decorate

    class BaseModel:
        def __init__(self, **values):
            cls, annotations = type(self), {}
            for klass in reversed(cls.__mro__):
                annotations.update(getattr(klass, "__annotations__", {}))
            for name in annotations:
                info = getattr(cls, name, ...)
                kw = info.kw if isinstance(info, _FieldInfo) else {}
                default = info.default if isinstance(info, _FieldInfo) else info
                if name in values:
                    value = values[name]
                elif default is ...:
                    raise ValidationError(f"{name} is required")
                else:
                    value = default
                if "max_length" in kw and value is not None and len(value) > kw["max_length"]:
                    raise ValidationError(f"{name} may have at most {kw['max_length']} items")
                if "pattern" in kw and isinstance(value, str):
                    import re
                    if not re.search(kw["pattern"], value):
                        raise ValidationError(f"{name} does not match {kw['pattern']}")
                setattr(self, name, value)
            for klass in cls.__mro__:
                for obj in vars(klass).values():
                    func = obj.__func__ if isinstance(obj, classmethod) else None
                    for name in getattr(func, "__validates__", ()):
                        setattr(self, name, func(cls, getattr(self, name)))

    fastapi.Depends, fastapi.FastAPI, fastapi.Header, fastapi.HTTPException = Depends, FastAPI, Header, HTTPException
    responses.FileResponse, responses.JSONResponse = FileResponse, JSONResponse
    staticfiles.StaticFiles = StaticFiles
    pydantic.BaseModel, pydantic.Field, pydantic.field_validator, pydantic.ValidationError = BaseModel, Field, field_validator, ValidationError
    fastapi.responses, fastapi.staticfiles = responses, staticfiles
    sys.modules.update({"fastapi": fastapi, "fastapi.responses": responses, "fastapi.staticfiles": staticfiles, "pydantic": pydantic})
    return True
