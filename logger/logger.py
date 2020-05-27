from abc import ABC, abstractproperty
import logging
import inspect
import time


class LoggerMixin(ABC):
    """ Mixin을 상속하여 logger를 쉽게 구현 가능 """

    LOGGERS = {}
    FILE_LEVEL = logging.DEBUG
    STREAM_LEVEL = logging.DEBUG

    @property
    def module(self) -> str:
        # Mixin을 상속한 모듈명
        return getattr(inspect.getmodule(self.__class__), "__name__")

    @property
    def top_level_module(self) -> str:
        return getattr(inspect.getmodule(self.__class__), "__name__").split(".")[0]

    @property
    def log_path(self) -> str:
        """ you can override this property 
        return None for no file logging
        """
        return f"logger/.logs/{self.module}.log"

    @property
    def logger(self):
        logger = LoggerMixin.LOGGERS.get(self.module)
        if logger:
            return logger

        # if logger doens't exist, build a new one
        logger_builder = LoggerBuidler(self.module)
        logger_builder.addFileHandler(path=self.log_path, level=self.FILE_LEVEL)
        logger_builder.addStreamHandler(level=self.STREAM_LEVEL)
        logger = logger_builder.build()

        LoggerMixin.LOGGERS[self.module] = logger
        return logger


class LoggerBuidler:
    _logger = None
    _fileHandler = None
    _streamHandler = None

    def __init__(self, name, format=""):
        if not format:
            format = "[%(module)s:%(lineno)d][%(levelname)s][%(asctime)s] %(message)s"

        self._logger = logging.getLogger(name)
        self._formatter = logging.Formatter(format)

    def build(self) -> logging.Logger:
        return self._logger

    def addFileHandler(self, path, level=logging.DEBUG, *args, **kwargs) -> None:
        if self._fileHandler is None and path is not None:
            fh = logging.FileHandler(path)
            fh.setLevel(level)
            fh.setFormatter(self._formatter)
            self._logger.addHandler(fh)

            self._fileHandler = fh

    def addStreamHandler(self, level=logging.DEBUG) -> None:
        if self._streamHandler is None:
            sh = logging.StreamHandler()
            sh.setLevel(level)
            sh.setFormatter(self._formatter)
            self._logger.addHandler(sh)

            self._streamHandler = sh


class ClassLogger(logging.getLoggerClass()):
    """ 기존 Logger 클래스와 통일성 위해 매서드 이름은 camelCase로 작성 """

    def __init__(self, name="", level=logging.DEBUG, *args, **kwargs):
        if not name:
            name = getattr(inspect.getmodule(self.__class__), "__name__")
        super(ClassLogger, self).__init__(name, level=level, *args, **kwargs)

    @staticmethod
    def debug_mode(func):
        def inner(*args, **kwargs):
            try:
                cls = args[0]
                cls_name = getattr(cls.__class__, "__name__")
                module = getattr(cls.__class__, "__module__")
                logger = logging.getLogger(name=module)
            except (IndexError, AttributeError):
                logger = logging.getLogger()

            stime = time.time()
            try:
                func_name = f"{cls_name}.{getattr(func, '__name__')}"
                logger.debug(f"{func_name} start")

                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    logger.error(e)
                    raise e()

                logger.debug(f"{func_name} end, running time: {time.time() - stime}")
                return result

            except Exception as e:
                logger.error(f"{func_name} {e}")

        return inner


logging.setLoggerClass(ClassLogger)
